//! `perf-counters` feature: deterministic frame, render, commit, save and
//! allocation counters for budget tests and the bench harness.
//!
//! Call sites use the crate-local `perf_count!` and `perf_scope!` macros,
//! which expand to nothing without the feature, so a default build carries no
//! counter code, statics or allocator wrapper. With it:
//!
//! - every counter is a relaxed atomic bumped on the UI thread;
//! - a `waku-perf-counters` thread closes a one-second bucket each second and,
//!   when `WAKU_PERF_COUNTERS_OUT` names a file, rewrites that file with the
//!   JSON report (temp file plus rename, so readers never see a torn write);
//! - the app writes a final report (`"final": true`) when it quits;
//! - the FPS counter overlay shows the last bucket.
//!
//! The JSON schema is documented in docs/performance.md; bump [`SCHEMA`] when
//! a field changes meaning. Added fields keep the version: readers must
//! ignore fields they do not know.

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::Serialize;

/// Identifies the report layout; budget tests should check it.
pub const SCHEMA: &str = "waku.perf-counters/1";
/// Environment variable naming the JSON report file.
pub const OUT_ENV: &str = "WAKU_PERF_COUNTERS_OUT";
/// One-second buckets kept for the report's `series`: an hour of history.
const SERIES_CAPACITY: usize = 3600;
const BUCKET: Duration = Duration::from_secs(1);

// ── Allocation counting ──────────────────────────────────────────────────

/// Counts every allocation, process-wide and per thread, before handing it
/// to the backing allocator (the system's, or mimalloc with the `mimalloc`
/// feature). `realloc` counts its full new size, as a fresh allocation would.
struct CountingAllocator;

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc as Backing;
#[cfg(not(feature = "mimalloc"))]
use std::alloc::System as Backing;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);

thread_local! {
    // Const-initialized and without a destructor, so touching it from the
    // allocator cannot allocate or fail during thread teardown.
    static THREAD_ALLOC_BYTES: Cell<u64> = const { Cell::new(0) };
}

#[inline]
fn record_allocation(bytes: usize) {
    let bytes = bytes as u64;
    ALLOC_BYTES.fetch_add(bytes, Relaxed);
    ALLOC_COUNT.fetch_add(1, Relaxed);
    let _ = THREAD_ALLOC_BYTES.try_with(|total| total.set(total.get().wrapping_add(bytes)));
}

fn thread_alloc_bytes() -> u64 {
    THREAD_ALLOC_BYTES.try_with(Cell::get).unwrap_or(0)
}

// SAFETY: every method forwards to `Backing` unchanged; the bookkeeping
// touches only atomics and a const thread-local, never the heap.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { Backing.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { Backing.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { Backing.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let pointer = unsafe { Backing.realloc(pointer, layout, new_size) };
        if !pointer.is_null() {
            record_allocation(new_size);
        }
        pointer
    }
}

// ── Counters ─────────────────────────────────────────────────────────────

static FRAMES: AtomicU64 = AtomicU64::new(0);
static RENDER_ROOT: AtomicU64 = AtomicU64::new(0);
static RENDER_SIDEBAR: AtomicU64 = AtomicU64::new(0);
static RENDER_TRANSCRIPT: AtomicU64 = AtomicU64::new(0);
static RENDER_RIGHT_PANEL: AtomicU64 = AtomicU64::new(0);
static RENDER_COMPOSER: AtomicU64 = AtomicU64::new(0);
static STREAM_COMMITS: AtomicU64 = AtomicU64::new(0);
static COMMIT_FRAMES: AtomicU64 = AtomicU64::new(0);
static TRANSCRIPT_ROWS_BUILT: AtomicU64 = AtomicU64::new(0);
static TRANSCRIPT_ROWS_BUILT_IN_COMMIT_FRAMES: AtomicU64 = AtomicU64::new(0);
static SAVE_CALLS: AtomicU64 = AtomicU64::new(0);
static SAVE_UI_NS: AtomicU64 = AtomicU64::new(0);
static SAVE_MAX_NS: AtomicU64 = AtomicU64::new(0);
static UI_ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static COMMIT_ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static SESSION_VISITS: AtomicU64 = AtomicU64::new(0);

/// Nanoseconds from [`init`] to the first frame and to the first transcript
/// row, plus one so zero can mean "not yet".
static FIRST_FRAME_NS: AtomicU64 = AtomicU64::new(0);
static FIRST_CONTENT_NS: AtomicU64 = AtomicU64::new(0);
/// Same encoding: to the startup thread having the daemon connected and the
/// task state, drafts and selected session loaded (#29).
static DAEMON_READY_NS: AtomicU64 = AtomicU64::new(0);

/// Record "now" in `cell` the first time only. One relaxed load per call
/// once set.
#[inline]
fn mark_first(cell: &AtomicU64) {
    if cell.load(Relaxed) != 0 {
        return;
    }
    if let Some(state) = STATE.get() {
        let nanos = u64::try_from(state.started.elapsed().as_nanos()).unwrap_or(u64::MAX - 1);
        let _ = cell.compare_exchange(0, nanos.saturating_add(1), Relaxed, Relaxed);
    }
}

/// Where the frame that presents a stream commit stands. A commit arms it;
/// the next root render (the start of that frame) moves it to `IN_FRAME`, so
/// rows built while laying that frame out are attributed to the commit; the
/// root render after that disarms it.
static COMMIT_FRAME: AtomicU8 = AtomicU8::new(COMMIT_FRAME_IDLE);
const COMMIT_FRAME_IDLE: u8 = 0;
const COMMIT_FRAME_ARMED: u8 = 1;
const COMMIT_FRAME_IN_FRAME: u8 = 2;

/// The root view rendered: one window frame. Also publishes the UI thread's
/// allocation total, since the root render always runs on the UI thread.
pub fn frame() {
    mark_first(&FIRST_FRAME_NS);
    FRAMES.fetch_add(1, Relaxed);
    RENDER_ROOT.fetch_add(1, Relaxed);
    UI_ALLOC_BYTES.store(thread_alloc_bytes(), Relaxed);
    let next = match COMMIT_FRAME.load(Relaxed) {
        COMMIT_FRAME_ARMED => {
            COMMIT_FRAMES.fetch_add(1, Relaxed);
            COMMIT_FRAME_IN_FRAME
        }
        _ => COMMIT_FRAME_IDLE,
    };
    COMMIT_FRAME.store(next, Relaxed);
}

pub fn render_sidebar() {
    RENDER_SIDEBAR.fetch_add(1, Relaxed);
}

pub fn render_transcript() {
    RENDER_TRANSCRIPT.fetch_add(1, Relaxed);
}

pub fn render_right_panel() {
    RENDER_RIGHT_PANEL.fetch_add(1, Relaxed);
}

pub fn render_composer() {
    RENDER_COMPOSER.fetch_add(1, Relaxed);
}

/// The transcript row builder ran for one row.
pub fn transcript_row() {
    mark_first(&FIRST_CONTENT_NS);
    TRANSCRIPT_ROWS_BUILT.fetch_add(1, Relaxed);
    if COMMIT_FRAME.load(Relaxed) == COMMIT_FRAME_IN_FRAME {
        TRANSCRIPT_ROWS_BUILT_IN_COMMIT_FRAMES.fetch_add(1, Relaxed);
    }
}

/// The startup thread finished: daemon connected, initial state loaded (or
/// the attempt failed). Runs off the UI thread.
pub fn daemon_ready() {
    mark_first(&DAEMON_READY_NS);
}

/// The bench harness's session visitor selected a session.
pub fn session_visit() {
    SESSION_VISITS.fetch_add(1, Relaxed);
}

/// Set while the current pump drain has applied streamed content.
static STREAM_DRAIN_PENDING: AtomicBool = AtomicBool::new(false);

/// One runtime's share of a pump drain finished; `streamed` is whether it
/// applied any text or reasoning delta. However many runtimes stream, the
/// drain counts as one commit when its [`PumpScope`] closes.
pub fn stream_drain(streamed: bool) {
    if streamed {
        STREAM_DRAIN_PENDING.store(true, Relaxed);
    }
}

/// Guard around one event-pump drain. A drain that applied streamed content
/// is one stream commit, and the UI-thread bytes it allocated count toward
/// `commit_alloc_bytes`.
pub struct PumpScope {
    alloc: u64,
}

pub fn pump_scope() -> PumpScope {
    STREAM_DRAIN_PENDING.store(false, Relaxed);
    PumpScope {
        alloc: thread_alloc_bytes(),
    }
}

impl Drop for PumpScope {
    fn drop(&mut self) {
        if STREAM_DRAIN_PENDING.swap(false, Relaxed) {
            STREAM_COMMITS.fetch_add(1, Relaxed);
            COMMIT_FRAME.store(COMMIT_FRAME_ARMED, Relaxed);
            COMMIT_ALLOC_BYTES.fetch_add(thread_alloc_bytes().wrapping_sub(self.alloc), Relaxed);
        }
    }
}

/// Guard timing one `Waku::save` on the UI thread.
pub struct SaveScope(Instant);

pub fn save_scope() -> SaveScope {
    SaveScope(Instant::now())
}

impl Drop for SaveScope {
    fn drop(&mut self) {
        let nanos = u64::try_from(self.0.elapsed().as_nanos()).unwrap_or(u64::MAX);
        SAVE_CALLS.fetch_add(1, Relaxed);
        SAVE_UI_NS.fetch_add(nanos, Relaxed);
        SAVE_MAX_NS.fetch_max(nanos, Relaxed);
    }
}

// ── Snapshots and buckets ────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct Renders {
    pub root: u64,
    pub sidebar: u64,
    pub transcript: u64,
    pub right_panel: u64,
    pub composer: u64,
}

impl Renders {
    fn delta(self, earlier: Self) -> Self {
        Self {
            root: self.root.saturating_sub(earlier.root),
            sidebar: self.sidebar.saturating_sub(earlier.sidebar),
            transcript: self.transcript.saturating_sub(earlier.transcript),
            right_panel: self.right_panel.saturating_sub(earlier.right_panel),
            composer: self.composer.saturating_sub(earlier.composer),
        }
    }

    fn max(self, other: Self) -> Self {
        Self {
            root: self.root.max(other.root),
            sidebar: self.sidebar.max(other.sidebar),
            transcript: self.transcript.max(other.transcript),
            right_panel: self.right_panel.max(other.right_panel),
            composer: self.composer.max(other.composer),
        }
    }

    fn per(self, divisor: u64) -> Option<RendersRatio> {
        let ratio = |value: u64| value as f64 / divisor as f64;
        (divisor > 0).then(|| RendersRatio {
            root: ratio(self.root),
            sidebar: ratio(self.sidebar),
            transcript: ratio(self.transcript),
            right_panel: ratio(self.right_panel),
            composer: ratio(self.composer),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct RendersRatio {
    pub root: f64,
    pub sidebar: f64,
    pub transcript: f64,
    pub right_panel: f64,
    pub composer: f64,
}

/// Cumulative counter values since the app started.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct Totals {
    pub frames: u64,
    pub renders: Renders,
    pub stream_commits: u64,
    pub commit_frames: u64,
    pub transcript_rows_built: u64,
    pub transcript_rows_built_in_commit_frames: u64,
    pub save_calls: u64,
    pub save_ui_ns: u64,
    pub save_max_ns: u64,
    pub alloc_bytes: u64,
    pub alloc_count: u64,
    pub ui_alloc_bytes: u64,
    pub commit_alloc_bytes: u64,
    pub session_visits: u64,
}

impl Totals {
    fn load() -> Self {
        Self {
            frames: FRAMES.load(Relaxed),
            renders: Renders {
                root: RENDER_ROOT.load(Relaxed),
                sidebar: RENDER_SIDEBAR.load(Relaxed),
                transcript: RENDER_TRANSCRIPT.load(Relaxed),
                right_panel: RENDER_RIGHT_PANEL.load(Relaxed),
                composer: RENDER_COMPOSER.load(Relaxed),
            },
            stream_commits: STREAM_COMMITS.load(Relaxed),
            commit_frames: COMMIT_FRAMES.load(Relaxed),
            transcript_rows_built: TRANSCRIPT_ROWS_BUILT.load(Relaxed),
            transcript_rows_built_in_commit_frames: TRANSCRIPT_ROWS_BUILT_IN_COMMIT_FRAMES
                .load(Relaxed),
            save_calls: SAVE_CALLS.load(Relaxed),
            save_ui_ns: SAVE_UI_NS.load(Relaxed),
            save_max_ns: SAVE_MAX_NS.load(Relaxed),
            alloc_bytes: ALLOC_BYTES.load(Relaxed),
            alloc_count: ALLOC_COUNT.load(Relaxed),
            ui_alloc_bytes: UI_ALLOC_BYTES.load(Relaxed),
            commit_alloc_bytes: COMMIT_ALLOC_BYTES.load(Relaxed),
            session_visits: SESSION_VISITS.load(Relaxed),
        }
    }
}

/// What happened during one bucket (normally one second).
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct Bucket {
    /// Bucket end, in milliseconds since the counters started.
    pub t_ms: u64,
    pub dur_ms: u64,
    pub frames: u64,
    pub renders: Renders,
    pub stream_commits: u64,
    pub transcript_rows_built: u64,
    pub save_calls: u64,
    pub save_ui_ns: u64,
    pub alloc_bytes: u64,
    pub commit_alloc_bytes: u64,
}

impl Bucket {
    fn between(earlier: &Totals, later: &Totals, t_ms: u64, dur_ms: u64) -> Self {
        Self {
            t_ms,
            dur_ms,
            frames: later.frames.saturating_sub(earlier.frames),
            renders: later.renders.delta(earlier.renders),
            stream_commits: later.stream_commits.saturating_sub(earlier.stream_commits),
            transcript_rows_built: later
                .transcript_rows_built
                .saturating_sub(earlier.transcript_rows_built),
            save_calls: later.save_calls.saturating_sub(earlier.save_calls),
            save_ui_ns: later.save_ui_ns.saturating_sub(earlier.save_ui_ns),
            alloc_bytes: later.alloc_bytes.saturating_sub(earlier.alloc_bytes),
            commit_alloc_bytes: later
                .commit_alloc_bytes
                .saturating_sub(earlier.commit_alloc_bytes),
        }
    }
}

/// Maximum count seen in any one complete bucket.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct Peaks {
    pub frames: u64,
    pub renders: Renders,
    pub stream_commits: u64,
    pub transcript_rows_built: u64,
    pub save_ui_ns: u64,
    pub alloc_bytes: u64,
}

impl Peaks {
    fn include(&mut self, bucket: &Bucket) {
        self.frames = self.frames.max(bucket.frames);
        self.renders = self.renders.max(bucket.renders);
        self.stream_commits = self.stream_commits.max(bucket.stream_commits);
        self.transcript_rows_built = self.transcript_rows_built.max(bucket.transcript_rows_built);
        self.save_ui_ns = self.save_ui_ns.max(bucket.save_ui_ns);
        self.alloc_bytes = self.alloc_bytes.max(bucket.alloc_bytes);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct Rates {
    pub frames: f64,
    pub renders: RendersRatio,
    pub stream_commits: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct PerCommit {
    pub transcript_rows_built: Option<f64>,
    pub alloc_bytes: Option<f64>,
    pub renders: Option<RendersRatio>,
}

/// Launch milestones. With `origin: "process"` every time is measured from
/// the moment the OS created the process (macOS); with `origin: "init"` the
/// process start is unknown and times run from [`init`].
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct Startup {
    pub origin: &'static str,
    /// Process start to [`init`] (early in `waku::run`): dyld, static
    /// initializers and `main`. `null` with `origin: "init"`.
    pub init_ms: Option<f64>,
    /// To the first root render: the first window frame being drawn, which
    /// is the launch shell when the daemon is not ready yet.
    pub first_frame_ms: Option<f64>,
    /// To the first transcript row built: the first frame with transcript
    /// content. `null` until a session with messages is on screen.
    pub first_content_ms: Option<f64>,
    /// To the startup thread delivering the daemon and the loaded state.
    /// Runs in parallel with GPUI start-up and the window opening.
    pub daemon_ready_ms: Option<f64>,
}

impl Startup {
    fn load(init_offset_ns: Option<u64>) -> Self {
        let offset = init_offset_ns.unwrap_or(0);
        let since_origin = |cell: &AtomicU64| match cell.load(Relaxed) {
            0 => None,
            plus_one => Some((offset + plus_one - 1) as f64 / 1e6),
        };
        Self {
            origin: if init_offset_ns.is_some() {
                "process"
            } else {
                "init"
            },
            init_ms: init_offset_ns.map(|nanos| nanos as f64 / 1e6),
            first_frame_ms: since_origin(&FIRST_FRAME_NS),
            first_content_ms: since_origin(&FIRST_CONTENT_NS),
            daemon_ready_ms: since_origin(&DAEMON_READY_NS),
        }
    }
}

/// How long this process had existed when [`init`] ran, from the kernel's
/// record of its start time.
#[cfg(target_os = "macos")]
fn process_age() -> Option<Duration> {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    // SAFETY: `info` is a writable buffer of exactly `size` bytes.
    let written = unsafe {
        libc::proc_pidinfo(
            libc::getpid(),
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if written != size {
        return None;
    }
    // SAFETY: the kernel filled all `size` bytes.
    let info = unsafe { info.assume_init() };
    let started = std::time::UNIX_EPOCH
        + Duration::from_secs(info.pbi_start_tvsec)
        + Duration::from_micros(info.pbi_start_tvusec);
    std::time::SystemTime::now().duration_since(started).ok()
}

#[cfg(not(target_os = "macos"))]
fn process_age() -> Option<Duration> {
    None
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Report {
    pub schema: &'static str,
    pub pid: u32,
    #[serde(rename = "final")]
    pub is_final: bool,
    pub elapsed_ms: u64,
    pub startup: Startup,
    pub totals: Totals,
    pub mean_per_s: Rates,
    pub peak_per_s: Peaks,
    pub per_commit: PerCommit,
    pub series_dropped: u64,
    pub series: Vec<Bucket>,
}

#[derive(Default)]
struct Series {
    buckets: VecDeque<Bucket>,
    dropped: u64,
    peaks: Peaks,
    last_totals: Totals,
    last_ms: u64,
}

impl Series {
    fn close_bucket(&mut self, totals: Totals, now_ms: u64) -> Bucket {
        let bucket = Bucket::between(
            &self.last_totals,
            &totals,
            now_ms,
            now_ms.saturating_sub(self.last_ms),
        );
        self.last_totals = totals;
        self.last_ms = now_ms;
        self.peaks.include(&bucket);
        if self.buckets.len() == SERIES_CAPACITY {
            self.buckets.pop_front();
            self.dropped += 1;
        }
        self.buckets.push_back(bucket);
        bucket
    }

    fn report(&self, totals: Totals, elapsed_ms: u64, is_final: bool, startup: Startup) -> Report {
        let seconds = elapsed_ms as f64 / 1000.0;
        let per_second = |value: u64| {
            if seconds > 0.0 {
                value as f64 / seconds
            } else {
                0.0
            }
        };
        let commits = totals.stream_commits;
        let per_commit = |value: u64| (commits > 0).then(|| value as f64 / commits as f64);
        Report {
            schema: SCHEMA,
            pid: std::process::id(),
            is_final,
            elapsed_ms,
            startup,
            totals,
            mean_per_s: Rates {
                frames: per_second(totals.frames),
                renders: RendersRatio {
                    root: per_second(totals.renders.root),
                    sidebar: per_second(totals.renders.sidebar),
                    transcript: per_second(totals.renders.transcript),
                    right_panel: per_second(totals.renders.right_panel),
                    composer: per_second(totals.renders.composer),
                },
                stream_commits: per_second(commits),
            },
            peak_per_s: self.peaks,
            per_commit: PerCommit {
                transcript_rows_built: (totals.commit_frames > 0).then(|| {
                    totals.transcript_rows_built_in_commit_frames as f64
                        / totals.commit_frames as f64
                }),
                alloc_bytes: per_commit(totals.commit_alloc_bytes),
                renders: totals.renders.per(commits),
            },
            series_dropped: self.dropped,
            series: self.buckets.iter().copied().collect(),
        }
    }
}

struct State {
    started: Instant,
    /// Process start to `started`, when the platform reports it.
    init_offset_ns: Option<u64>,
    out: Option<PathBuf>,
    series: Mutex<Series>,
    /// The most recent complete bucket, for the overlay.
    last: Mutex<Option<Bucket>>,
}

static STATE: OnceLock<State> = OnceLock::new();

/// Start the bucket thread. Call once, early in `run`.
pub fn init() {
    let out = std::env::var_os(OUT_ENV)
        .filter(|path| !path.is_empty())
        .map(PathBuf::from);
    let started = Instant::now();
    let init_offset_ns = process_age().and_then(|age| u64::try_from(age.as_nanos()).ok());
    let state = State {
        started,
        init_offset_ns,
        out,
        series: Mutex::new(Series {
            last_totals: Totals::load(),
            ..Series::default()
        }),
        last: Mutex::new(None),
    };
    if STATE.set(state).is_err() {
        return;
    }
    let spawned = std::thread::Builder::new()
        .name("waku-perf-counters".into())
        .spawn(|| {
            let Some(state) = STATE.get() else {
                return;
            };
            let mut next = state.started + BUCKET;
            loop {
                std::thread::sleep(next.saturating_duration_since(Instant::now()));
                next += BUCKET;
                let now_ms = elapsed_ms(state);
                let totals = Totals::load();
                let report = {
                    let Ok(mut series) = state.series.lock() else {
                        return;
                    };
                    let bucket = series.close_bucket(totals, now_ms);
                    if let Ok(mut last) = state.last.lock() {
                        *last = Some(bucket);
                    }
                    state.out.as_ref().map(|_| {
                        series.report(totals, now_ms, false, Startup::load(state.init_offset_ns))
                    })
                };
                if let (Some(report), Some(path)) = (report, state.out.as_deref()) {
                    write_report(path, &report);
                }
            }
        });
    if let Err(error) = spawned {
        eprintln!("perf-counters: could not start the bucket thread: {error}");
    }
}

fn elapsed_ms(state: &State) -> u64 {
    u64::try_from(state.started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Write the final report, if `WAKU_PERF_COUNTERS_OUT` is set. Runs once at
/// quit, a one-shot action, so the synchronous write is acceptable there.
pub fn write_final() {
    let Some(state) = STATE.get() else {
        return;
    };
    let Some(path) = state.out.as_deref() else {
        return;
    };
    let now_ms = elapsed_ms(state);
    let totals = Totals::load();
    let report = match state.series.lock() {
        Ok(series) => series.report(totals, now_ms, true, Startup::load(state.init_offset_ns)),
        Err(_) => return,
    };
    write_report(path, &report);
}

fn write_report(path: &Path, report: &Report) {
    let result = serde_json::to_vec_pretty(report)
        .map_err(std::io::Error::other)
        .and_then(|json| {
            let mut temporary = path.as_os_str().to_owned();
            temporary.push(".tmp");
            let temporary = PathBuf::from(temporary);
            std::fs::write(&temporary, json)?;
            std::fs::rename(&temporary, path)
        });
    if let Err(error) = result {
        eprintln!("perf-counters: could not write {}: {error}", path.display());
    }
}

/// One line for the debug overlay next to the FPS counter: the last complete
/// bucket, or `None` before the first one closes. Never blocks the frame.
pub fn overlay_text() -> Option<String> {
    let bucket = (*STATE.get()?.last.try_lock().ok()?)?;
    let renders = bucket.renders;
    Some(format!(
        "{}c/s · R t{} s{} p{} c{} · rows {} · save {:.1}ms",
        bucket.stream_commits,
        renders.transcript,
        renders.sidebar,
        renders.right_panel,
        renders.composer,
        bucket.transcript_rows_built,
        bucket.save_ui_ns as f64 / 1_000_000.0,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn totals(frames: u64, commits: u64, transcript: u64) -> Totals {
        Totals {
            frames,
            renders: Renders {
                root: frames,
                transcript,
                ..Renders::default()
            },
            stream_commits: commits,
            commit_frames: commits,
            transcript_rows_built_in_commit_frames: commits * 4,
            commit_alloc_bytes: commits * 1000,
            ..Totals::default()
        }
    }

    #[test]
    fn buckets_are_deltas_and_peaks_track_the_busiest_second() {
        let mut series = Series::default();
        series.close_bucket(totals(60, 8, 8), 1000);
        series.close_bucket(totals(180, 10, 10), 2000);
        let report = series.report(totals(180, 10, 10), 2000, true, Startup::load(None));
        assert_eq!(report.series.len(), 2);
        assert_eq!(report.series[1].frames, 120);
        assert_eq!(report.series[1].stream_commits, 2);
        assert_eq!(report.series[1].dur_ms, 1000);
        assert_eq!(report.peak_per_s.frames, 120);
        assert_eq!(report.peak_per_s.stream_commits, 8);
        assert_eq!(report.mean_per_s.frames, 90.0);
        assert_eq!(report.per_commit.transcript_rows_built, Some(4.0));
        assert_eq!(report.per_commit.alloc_bytes, Some(1000.0));
        assert_eq!(report.per_commit.renders.map(|r| r.transcript), Some(1.0));
    }

    #[test]
    fn series_is_bounded() {
        let mut series = Series::default();
        for second in 1..=(SERIES_CAPACITY as u64 + 5) {
            series.close_bucket(totals(second, 0, 0), second * 1000);
        }
        assert_eq!(series.buckets.len(), SERIES_CAPACITY);
        assert_eq!(series.dropped, 5);
    }

    #[test]
    fn report_serializes_the_documented_fields() {
        let series = Series::default();
        let json = serde_json::to_value(series.report(
            Totals::default(),
            0,
            false,
            Startup::load(Some(5_000_000)),
        ))
        .unwrap();
        assert_eq!(json["schema"], SCHEMA);
        assert_eq!(json["final"], false);
        for field in [
            "pid",
            "elapsed_ms",
            "startup",
            "totals",
            "mean_per_s",
            "peak_per_s",
            "per_commit",
            "series_dropped",
            "series",
        ] {
            assert!(json.get(field).is_some(), "missing {field}");
        }
        assert!(json["per_commit"]["alloc_bytes"].is_null());
        assert_eq!(json["startup"]["origin"], "process");
        assert_eq!(json["startup"]["init_ms"], 5.0);
        assert!(json["startup"]["daemon_ready_ms"].is_null());
        assert_eq!(json["totals"]["session_visits"], 0);
        assert_eq!(json["totals"]["renders"]["right_panel"], 0);
    }

    #[test]
    fn commit_frame_attributes_rows_to_the_frame_after_a_commit() {
        let before = Totals::load();
        {
            let _pump = pump_scope();
            stream_drain(false);
            stream_drain(true);
            stream_drain(true);
        }
        frame();
        transcript_row();
        transcript_row();
        frame();
        transcript_row();
        let after = Totals::load();
        assert_eq!(after.stream_commits - before.stream_commits, 1);
        assert_eq!(after.commit_frames - before.commit_frames, 1);
        assert_eq!(
            after.transcript_rows_built_in_commit_frames
                - before.transcript_rows_built_in_commit_frames,
            2
        );
        assert_eq!(
            after.transcript_rows_built - before.transcript_rows_built,
            3
        );
    }

    #[test]
    fn counting_allocator_sees_this_threads_allocations() {
        let before = thread_alloc_bytes();
        let buffer = std::hint::black_box(vec![0_u8; 4096]);
        assert!(thread_alloc_bytes() - before >= 4096);
        drop(buffer);
    }
}

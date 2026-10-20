# Streaming render performance

How Waku keeps CPU flat while a provider streams, what each piece of the
pipeline is allowed to cost, and how to measure before changing any of it.
This encodes the results of the 2026-08-16 streaming investigation, which took
sustained streaming CPU from 40–60% to under ~10% average (debug build) across
text and fast-reasoning streams.

The one-line model: **CPU ≈ redraw rate × visible element count.** GPUI
rebuilds and lays out every visible element on every frame a view renders —
[`list()`](https://github.com/egoist/zed) re-renders and `layout_as_root`s
every visible row per frame, cached heights only spare the overdraw — so every
rule below either bounds how often frames happen or how much is visible inside
one.

## Who is allowed to cause a frame

A frame happens when an entity is notified, when `window.refresh()` is called,
or when something re-arms `request_animation_frame`. Each has a different
price:

| Trigger | Cost | Allowed users |
| --- | --- | --- |
| `cx.notify(view)` | Re-renders that view and its ancestors; **cached sibling panes replay** | The stream pump (per commit), the pulse clock, user-event handlers |
| `window.refresh()` | Re-renders everything and **bypasses every cached pane** | Genuine whole-window invalidation only: hover transitions, drags, theme |
| `request_animation_frame` | Display-rate (120 Hz) re-render of the current view for as long as it re-arms | Nothing during streaming. One mounted repeating `with_animation` pinned the window at 120 Hz for a whole turn (~36% CPU by itself). The one sanctioned transient: the 200 ms panel show/hide slide ([src/app/render.rs](../src/app/render.rs)), which is off by default (**Animate panels** in settings), re-arms only while an edge is moving and gates the pane fan-out (below) |

The root `Waku` view re-renders on every frame regardless of what is dirty, so
it must stay thin: the sidebar, transcript, and right panel are `WakuPane`
islands ([src/app.rs](../src/app.rs)) embedded with the fork's
`Entity::cached`. Each pane observes the root — any root notify still
re-renders every island, so caching can never show stale state — while a
notify targeted at one pane (the pulse clock leases `window.current_view()`)
rebuilds only that island and replays the rest.

Producers that know what they changed notify islands directly instead of the
root ([src/app/dirty.rs](../src/app/dirty.rs)). The event pump builds a
`DirtySet { root, sidebar, transcript, right_panel }` per drain:

- text and reasoning deltas of the selected session dirty the transcript;
- tool activity and background work also dirty the right panel;
- a background session's deltas dirty nothing;
- the sidebar is dirty only when a drained session's sidebar fingerprint
  (`sidebar_session_fingerprint`: status, title, recency, branch, last turn)
  moved;
- everything else (turn boundaries, permissions, errors, usage, session
  added or removed, the other result queues) is `DirtySet::ALL`, a plain root
  notify.

`Waku::notify_dirty` notifies the named panes, or uses a plain root notify
when all three are dirty. The root renders on every draw anyway, so root
state rides along with any pane notify. When only the root is dirty, it
notifies `RootFrame`, a zero-size child of the root: gpui marks it and its
ancestors dirty, so the root repaints and every island replays. Pulse ticks
and overlay-scrollbar wakes aimed at the root go to the same stand-in
(`motion::redirect_notifications`), so a spinner in the header or composer
no longer rebuilds all three islands at pulse rate. The time-label wake
dirties the root, sidebar and transcript, not the right panel.

The one exception to that fan-out is the 200 ms panel slide. Panels toggle
instantly unless the user turns on **Animate panels**; with it on, the slide's
display-rate root notifies would price every tick at a three-island rebuild,
so while `panels_sliding()` the observer skips the fan-out and the cached-view
keys decide instead. The sliding panel (its clip moves) misses its cache and
rebuilds with fresh state; the island nothing is moving replays. The
transcript keeps its *settled* width for the whole slide
(`transcript_slide_width`): animating the column's width used to change the
`list()` width every frame, which drops every cached row height and re-wraps
every visible paragraph at display rate. Held at the settled width, a right
panel slide leaves the transcript's bounds alone (it replays), and a sidebar
slide only moves its origin (it rebuilds rows, but heights and the line-layout
cache stay warm). Updates born inside an island still land
mid-slide because a child notify dirties its ancestor pane without the
observer, and the slide's retirement schedules one ungated notify so any
root-state drift in a reused pane converges the frame after the slide ends. Two traps to know: gpui's
`mark_view_dirty` walks **ancestors only**, which is why panes must observe
the root rather than expect root notifies to reach them; and a cached pane
lays its content out **as a root**, so a `flex_1`-sized subtree collapses to
its zero flex basis without a `size_full` flex wrapper.

## The two cadences

Everything during a stream happens at one of two rates, and every change to
the pipeline must keep it that way:

**Commits, ≤ ~8.3 Hz.** Provider chunks queue for a full
`STREAM_FRAME_INTERVAL` (120 ms) and fold into one drain → one notify → one
tail remeasure ([src/app.rs](../src/app.rs),
[src/app/runtime.rs](../src/app/runtime.rs)).
Two hard-won rules:

- The pump timer must **not** race the wake channel. It used to, which made
  the notify rate equal the provider's chunk rate.
- Every *streaming* delta kind must set the flags that route the pump onto the
  `StreamFrame` schedule. `ReasoningDelta` originally did not set
  `markdown_changed`, so a reasoning-only drain reported `Idle`, the pump went
  back to sleeping on the wake channel, and every fast thinking chunk woke it
  for an immediate drain-and-notify — **40+ commits and full re-renders per
  second**, sailing straight past the floor. Fast thinking hitting 40% CPU
  while text streamed at 10% was this one flag.

**Pulse ticks, ≤ 60 Hz.** All repeating animation rides the shared
self-parking clock in [src/ui/motion.rs](../src/ui/motion.rs): loaders read
a phase from a shared epoch, leases expire 300 ms after the loader last
painted, and the clock parks when no leases remain. Never use
`with_animation(...).repeat()` — it re-arms `request_animation_frame` every
display frame. A view's whole subtree rebuilds per tick, so cadence is priced
per *view*, not per animation: spinners use the full 60 Hz rate, while
`spin_slow` uses every second tick (≈ 30 Hz). Non-spinning pulses and
`pulse_lease` retain ≈ 30 Hz; `pulse_lease_slow` and `Pulse::every(2)` use
≈ 15 Hz for loaders mounted on expensive surfaces — the working dots set the
transcript pane's tick floor for the entire turn. Strides re-establish on
every tick (a lease's stride resets after it fires); an earlier version kept
the minimum stride forever, so one full-rate lease permanently dragged its
pane back to 30 Hz.

The veil dissolve is a pulse-clock client like everything else: the message
veil at ≈ 30 Hz, the reasoning veil at ≈ 15 Hz, both leasing
`window.current_view()` so a dissolve only rebuilds the island that hosts it.

**Overlay scrollbars are the classic violator of both cadences.** A streaming
surface moves its content every commit, so the bar sits in its reveal hold for
the whole turn — and the hold is constant-opacity, needing zero repaints.
[src/ui/scrollbar.rs](../src/ui/scrollbar.rs) therefore schedules a single
one-shot wake for hold expiry and rides the pulse clock only through the
350 ms fade. Driving frames through the hold pinned the pane at pulse rate the
moment any scrollbar became visible.

Scrolling is island-local too. `ListState::scroll` notifies the view that
painted the list, so the transcript's scroll handlers only record intent, and
the scrollbar's hover, click and drag listeners notify the view captured at
paint (`window.current_view()`). Neither calls `window.refresh()`, which used
to rebuild the sidebar and the right panel (terminal, diff, editor) on every
scroll tick.

## Bounding what is visible

- The transcript is virtualized with `list()`; per-commit invalidation is the
  last `STREAM_REMEASURE_TAIL_ROWS` rows only, and row folds, navigation
  turns, response footers, and sidebar rows are all fingerprint-cached
  ([src/app/transcript.rs](../src/app/transcript.rs),
  [src/app/sidebar.rs](../src/app/sidebar.rs)). A fingerprint must hash at
  display granularity: the sidebar row cache keys session recency, and hashing
  raw seconds would bust it on every commit.
- The live reasoning peek renders a **byte window** of the tail
  (`live_reasoning_window_start`,
  [src/app/transcript_view.rs](../src/app/transcript_view.rs)): markdown cost
  is O(rendered source) per tick regardless of block shape — a wall-of-text
  think is one giant paragraph and a bulleted think one giant list, so a
  block-count cap bounds neither. The slide hysteresis is wide
  (`LIVE_REASONING_WINDOW_MAX`) because fast reasoning appends several KB per
  commit and each slide rebuilds the window from a fresh view. The full trace
  renders once the turn settles.
- `markdown_tail` and block-index element ordinals
  (`block_ix << 16 | position`, [src/md/render.rs](../src/md/render.rs)) let
  a capped walk hand settled blocks the same flatten-cache and veil keys as a
  full walk.
- `MarkdownView::set_text` derives the mended display tail only when content
  or the streaming flag changed — the derivation re-parses the final block and
  runs for every visible row every frame.

## Markdown math

Inline `$…$` and display `$$…$$` formulas use the native RaTeX engine
([src/md/math.rs](../src/md/math.rs)). Parsing TeX, loading embedded font
outlines, producing SVGs and rasterizing them all run on the background
executor. A frame only queues cache misses and reads completed results.
The General setting **Render math expressions** is enabled by default.
When disabled, every Markdown surface uses selectable LaTeX text without
queuing math jobs. Right-clicking a rendered formula adds **Copy Expression**
to its context menu; the copied source is captured at the click so later
streaming or layout changes cannot change the expression being copied.

Two workers process batches of up to eight formulas. Pending work is
deduplicated across Markdown views and capped at 128 requests. The image
cache retains at most 256 entries / 32 MiB, including failed results so
invalid formulas never retry on every frame. Eviction explicitly releases
GPUI sprite-atlas entries as well as CPU images. A separate bounded cache
keeps color-independent typesetting for theme and display-scale changes.
Individual formulas are limited to 8 KiB of source and two million raster
pixels; pending, invalid or oversized formulas keep selectable source text.

Ordinary prose retains the existing `StyledText` path. A paragraph containing
math uses one measured element with cached glyph runs and line positions,
including baseline alignment and Unicode wrapping. It does not create an
element per word or start an animation clock. Selection and search use the
original LaTeX byte ranges. The manual measurement is
`cargo test --locked -p waku --lib md::math::tests::benchmark_native_math_cache -- --ignored --nocapture`.

## Measuring

Sampling alone misled this investigation for hours; counters cracked it in one
run. In order of usefulness:

1. **Per-second counters, logged from render.** Static `AtomicU32`s counting
   window frames, per-pane renders, and pump commits, flushed once per second
   to a file from the root render (write on the background executor). One
   streaming run gives the full decomposition — it is how the 40-commits/sec
   reasoning bug and the stuck-stride bug were found after profiles showed
   nothing but generic layout work. The `perf-counters` feature (below)
   is this, built in and compiled out of default builds.
2. **`sample <pid> 5` during a captured stream**, `awk '/Sort by top of
   stack/,0'` for leaves, ancestor-walk for the hot chain. Good for *what*
   is expensive (taffy vs shaping vs app code), useless for *how often*.
   The production binary is stripped — profile the debug build, or a
   `--profile profiling` build (below) for release code with symbols.
3. **CPU traces across a whole turn**: poll `ps -o %cpu= -p <pid>` every
   500 ms from trigger until settle. Averages over a turn hide phase
   plateaus; report both.
4. The built-in FPS counter pins the window at display rate by design and
   cannot measure streaming cadence.

Debug-build numbers overweight taffy/style/scene generics by several fold;
treat them as structure, not as what users feel, and confirm user-facing
claims on a release build.

### Perf counters (`perf-counters` feature)

Measuring method 1 above, built in. The `perf-counters` feature on the root
crate adds relaxed atomic counters on the UI thread, a counting wrapper
around the system allocator, and a thread that closes a one-second bucket
every second. Call sites use the `perf_count!`/`perf_scope!` macros
([src/lib.rs](../src/lib.rs)), which expand to nothing without the feature,
so a default build has no counters, no allocator wrapper and no thread.

```sh
WAKU_PERF_COUNTERS_OUT=/tmp/waku-perf.json \
  cargo run --profile profiling --features perf-counters
```

- With `WAKU_PERF_COUNTERS_OUT` set, the JSON report is rewritten once per
  second (temp file plus rename, so a reader never sees a partial file) with
  `"final": false`, and once more at quit with `"final": true`. A scripted run
  can read it while the app is still up, or quit the app and read the final
  report.
- The FPS counter (**View › Toggle FPS Counter**, `cmd-alt-shift-f`; `ctrl-alt-shift-f` off
  macOS) shows the
  last bucket next to the FPS reading: `8c/s · R t9 s0 p0 c9 · rows 14 ·
  save 0.0ms` is stream commits, then transcript/sidebar/right-panel/composer
  renders, transcript rows built and UI time in save, all for the last
  second. The FPS counter pins the window at display rate, so turn it off
  for runs whose numbers you keep.
- The counters are process-local to the app; the daemon has none.

Definitions:

| Counter | Counted where |
| --- | --- |
| frame | Each `Waku::render`. The root re-renders on every window draw, so this is the frame count, and `renders.root` equals it. |
| `renders.sidebar` / `transcript` / `right_panel` | The `WakuPane` delegates. A cached pane replaying its subtree is not a render. |
| `renders.composer` | `ComposerInput::render`. |
| stream commit | A pump drain (`drain_event_pump`) that applied at least one text or reasoning delta, however many sessions streamed in it. |
| commit frame | The first frame after a stream commit: the commit's root notify, presented. |
| transcript row built | Each `transcript_row` call (the `list()` row builder). |
| save | Each `Waku::save`, timed on the UI thread. |
| allocation | Each `alloc`, `alloc_zeroed` or `realloc` (counted at its full new size). |

Report schema (`"schema": "waku.perf-counters/1"`). Counts are integers;
`_ns` fields are nanoseconds, `_ms` milliseconds, `_bytes` bytes. Ratios are
`null` when their denominator is zero. Fields are only ever added within a
schema version (readers ignore what they do not know); a field that changes
meaning bumps it. `startup` and `totals.session_visits` were added for the
baseline harness (#9).

| Field | Meaning |
| --- | --- |
| `schema`, `pid`, `final` | Layout version, app process id, and whether this is the report written at quit. |
| `elapsed_ms` | Time since the counters started (early in `waku::run`). |
| `startup.origin` | `"process"` when the times below run from the moment the OS created the process (macOS, from `proc_pidinfo`); `"init"` when the process start is unknown and they run from counter start. |
| `startup.init_ms` | Process start to counter start: exec, dyld, static initializers and `main` up to `waku::run`. `null` with `origin: "init"`. |
| `startup.first_frame_ms` | To the first root render, the first window frame being drawn (not yet presented). |
| `startup.first_content_ms` | To the first transcript row built, i.e. the first frame drawing transcript content. `null` until a session with messages is on screen. |
| `totals.frames` | Frames since start. |
| `totals.renders.{root,sidebar,transcript,right_panel,composer}` | Renders since start. |
| `totals.stream_commits` | Stream commits since start. |
| `totals.commit_frames` | Commit frames since start. |
| `totals.transcript_rows_built` | Rows built since start. |
| `totals.transcript_rows_built_in_commit_frames` | Rows built while laying out commit frames. |
| `totals.save_calls`, `totals.save_ui_ns`, `totals.save_max_ns` | `Waku::save` calls, total UI-thread time in them, and the longest single call. |
| `totals.alloc_bytes`, `totals.alloc_count` | Bytes and allocations, all threads. |
| `totals.ui_alloc_bytes` | Bytes allocated on the UI thread, as of the last frame. |
| `totals.session_visits` | Sessions selected by the bench visitor (`WAKU_PERF_VISIT_SESSIONS_MS`, below). |
| `totals.commit_alloc_bytes` | UI-thread bytes allocated inside the pump drains that were stream commits. |
| `mean_per_s.{frames,stream_commits}`, `mean_per_s.renders.*` | Totals divided by `elapsed_ms / 1000`. |
| `peak_per_s.{frames,stream_commits,transcript_rows_built,save_ui_ns,alloc_bytes}`, `peak_per_s.renders.*` | The highest value in any one complete one-second bucket. |
| `per_commit.transcript_rows_built` | `transcript_rows_built_in_commit_frames / commit_frames`: rows rebuilt to present one commit. |
| `per_commit.alloc_bytes` | `commit_alloc_bytes / stream_commits`: UI-thread bytes to apply one commit (not to render it). |
| `per_commit.renders.*` | Total renders divided by `stream_commits`. This is meaningful only for a run that does nothing but stream, which is what the budget harness drives. |
| `series[]` | One-second buckets, oldest first, at most 3600 (`series_dropped` counts the evicted ones). Each has `t_ms` (bucket end, since start), `dur_ms`, and the per-bucket deltas `frames`, `renders.*`, `stream_commits`, `transcript_rows_built`, `save_calls`, `save_ui_ns`, `alloc_bytes` and `commit_alloc_bytes`. |

With `WAKU_PERF_VISIT_SESSIONS_MS=<ms>` set, a `perf-counters` build also
selects every started session once, in catalog order, one per interval,
through the same path as a sidebar click. The memory scenarios of
`scripts/bench/baseline.sh` use it to visit N long sessions without UI input.
Default builds have neither the variable nor the code.

Budget checks map onto it directly. For example, "commits/s ≤ budget" is
`peak_per_s.stream_commits`, "transcript renders per commit ≤ 1" is
`per_commit.renders.transcript`, "sidebar and right panel do not render while
streaming" is `totals.renders.sidebar` and `right_panel` over a streaming-only
run, and "no UI time in save" is `totals.save_ui_ns`.

### Tracing spans and Tracy (`profiling` feature)

The app, `waku-core` and `waku-daemon` carry `tracing` spans on the paths a
streaming frame or a save reaches:

| Process | Spans |
| --- | --- |
| App | `drain_event_pump`, `Waku::render`, `sidebar_pane`, `transcript_pane`, `right_panel_pane`, `ComposerInput::render`, `transcript_row`, `MarkdownView::set_text`, `IncrementalParser::set_text`, `md::flatten`, `Waku::save` |
| Daemon (`waku-core`) | `Hub::emit`, `dispatch`, `handle_request`, `save_task_state`, `StateStore::save` |

Spans are written with the crate-local `profile_span!` macro. Without the
`profiling` feature it expands to nothing, so default and release builds carry
neither the spans nor `tracing` itself, and no subscriber is installed. With
the feature, a `tracing-tracy` layer records every span (all levels):

```sh
# Daemon: the app spawns the `waku-daemon` binary next to its own executable
# (target/profiling/) unless WAKU_DAEMON_PATH says otherwise, so build that
# one with the feature first to see its spans.
cargo build --profile profiling -p waku-daemon --features profiling
# App (the GUI process)
cargo run --profile profiling --features profiling
```

- The client is `tracy-client` 0.19, so connect with the **Tracy 0.14.1**
  profiler. It runs `ondemand` (nothing is buffered until a profiler
  attaches) and `only-localhost`. The daemon is a second Tracy client and
  appears separately in the profiler's discovery list.
- `WAKU_TRACE` (profiling builds only) adds a stderr layer that prints each
  matching span's busy and idle time when it closes. The value is a target
  filter such as `waku::app=debug,waku::md=trace` or
  `waku_core::server=trace`; `1`, `true` or `all` select everything. The
  daemon inherits the variable, and its stderr reaches the app's.
- `[profile.profiling]` inherits `release` and keeps line tables and symbols
  (`debug = "line-tables-only"`, `strip = false`), so `sample` output is
  symbolized too.

### Microbenchmarks and the CI gate

Criterion benches cover the window-free hot paths. Their inputs are generated
from fixed seeds (`benches/support/fixtures.rs`).

- `benches/desktop.rs` (`waku`, through `waku::bench_support`):
  - `IncrementalParser::append` over 20k streamed tokens;
  - `flatten` of a 3,000-line paragraph;
  - code runs and `highlight::tokenize` on a 3,000-line block;
  - `review_diff::parse_collected` on a 10k-line patch;
  - the terminal snapshot of a 200×60 grid.
- `crates/waku-protocol/benches/wire.rs`: encode and decode of 10k sequenced
  events.
- `crates/waku-core/benches/persistence.rs`: the daemon's `StateStore::save`
  for a 2k-message session, both the first write and the streaming re-save.

`tests/md_stream_linear.rs` asserts that streaming is linear. It counts
allocations, which is deterministic, so it does not depend on timing.

```sh
cargo bench --workspace --bench '*'              # everything
cargo bench -p waku --bench desktop -- md/       # one group
scripts/bench/compare.sh main .                  # A/B: main vs working tree
BENCH_FILTER=terminal/ BENCH_ROUNDS=2 scripts/bench/compare.sh HEAD~1 HEAD
```

Use `--bench '*'` rather than `--benches`. The latter also runs the libtest
harnesses, which reject Criterion's flags.

The `Benchmarks` workflow (`.github/workflows/bench.yml`) runs
`compare.sh <base sha> HEAD` on pull requests that touch `src/md`,
`src/terminal.rs`, `src/review_diff.rs`, `crates/`, or the benches themselves.
It builds both revisions on the same runner and runs them in alternating
rounds (three by default). It then takes the median of each benchmark's
per-round Criterion medians. A benchmark fails the job only when both hold:

- its median is more than 10% slower;
- the slowdown is larger than the spread between that runner's own rounds.

The report is written to the job summary.

## Known floor and next levers

With both cadences enforced, a streaming frame still rebuilds every visible
row (gpui `list()` semantics). If that ever needs to shrink: fork-level cached
list rows need a measure-once extension to `ViewElement` caching (cached views
lay out from style, not content, which breaks the list's measurement as-is);
alternatively fold activities into the virtualized list as block-granularity
rows. Smaller levers, in memory and unproven: stable
`StyledText` element ids for gpui's per-element layout memo, and the per-row
`Message` clones in the row builder.

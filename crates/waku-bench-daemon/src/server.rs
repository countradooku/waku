//! Starting and stopping a bench daemon, for the CLI and for tests.
//!
//! The bench daemon is `waku-daemon` as its `main` composes it — the real
//! `WakuBackend`, `DaemonHost` (orchestrator, effect workers, provider
//! session manager, lifecycle service, blob sweeps) and `serve_listeners`,
//! protocol v8 only — over a throwaway data directory. Only two things
//! differ:
//!
//! - the Codex and Claude provider binaries are `fake-codex` and
//!   `fake-claude`, which replay timelines through each provider's own
//!   protocol; every other provider is disabled, so no real provider CLI can
//!   start;
//! - the data directory is seeded with the fixture threads
//!   ([`crate::fixture`]) before the first connection.

use std::collections::{HashSet, VecDeque};
use std::io::{BufRead as _, BufReader};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{Context as _, bail};
use parking_lot::Mutex;
use serde_json::json;
use uuid::Uuid;
use waku_core::daemon::WakuBackend;
use waku_core::daemon_host::{DaemonHost, HostOptions};
use waku_core::{DaemonSettingsStore, ServerOptions};
use waku_orchestrator::Orchestrator;
use waku_orchestrator::feed::{CommittedFeed, FeedEnded};
use waku_protocol::model::ProviderKind;
use waku_protocol::orchestration::{DomainEvent, Run, RunId, RunStatus};

use crate::autoplay::{self, AutoplayOptions};
use crate::fixture::{self, FixtureOptions};
use crate::native::CONFIG_FILE;
use crate::replay::{Rate, ReplayStats};
use crate::timeline::{BUILTIN_NAMES, Timeline};

/// Marks a state directory as owned by the bench, so it may be wiped.
const STATE_MARKER: &str = ".waku-bench-daemon";
/// The fakes' replay statistics, one JSON line per replay.
const STATS_FILE: &str = "replays.jsonl";
/// Providers the bench drives through its fakes. Every other provider is
/// disabled in the bench daemon's settings.
pub const BENCH_PROVIDERS: [ProviderKind; 2] = [ProviderKind::Codex, ProviderKind::Claude];

pub struct BenchOptions {
    /// TCP address to listen on. With neither this nor `socket`, the daemon
    /// listens on `127.0.0.1:0`.
    pub listen: Option<String>,
    /// Local socket to listen on.
    pub socket: Option<PathBuf>,
    pub token: String,
    /// The default timeline: a built-in name, or a `.jsonl` / `.jsonl.gz`
    /// file, which joins the fixture as its own thread.
    pub timeline: String,
    pub rate: Rate,
    pub loops: u32,
    /// The provider of the fixture threads.
    pub provider: ProviderKind,
    pub history_turns: usize,
    pub history_sessions: usize,
    /// Stop serving after this many runs have replayed a timeline.
    pub exit_after: Option<usize>,
    /// `--autoplay`: prompt the default timeline's thread this long after a
    /// client subscribes to it. See [`crate::autoplay`].
    pub autoplay: Option<Duration>,
    /// Throwaway daemon data directory. Recreated on every start.
    pub state_dir: PathBuf,
    /// Project directory of the fixture; defaults to one inside `state_dir`.
    pub project_dir: Option<PathBuf>,
    /// Where `fake-codex` and `fake-claude` are; defaults to this
    /// executable's directory.
    pub fakes_dir: Option<PathBuf>,
    /// Desktop-owned daemons accept `daemon.shutdown`; so does the bench, so
    /// quitting the app ends the run and prints the summary.
    pub allow_shutdown: bool,
}

impl BenchOptions {
    pub fn new(state_dir: PathBuf, token: impl Into<String>) -> Self {
        Self {
            listen: None,
            socket: None,
            token: token.into(),
            timeline: "prose".into(),
            rate: Rate::default(),
            loops: 1,
            provider: ProviderKind::Codex,
            history_turns: 0,
            history_sessions: 1,
            exit_after: None,
            autoplay: None,
            state_dir,
            project_dir: None,
            fakes_dir: None,
            allow_shutdown: true,
        }
    }
}

/// One run that ended, with the replay its provider played for it.
#[derive(Clone, Debug)]
pub struct RunReport {
    pub thread_id: Uuid,
    pub run_id: RunId,
    pub status: RunStatus,
    /// From the run's request to its end, in daemon time.
    pub run_ms: Option<i64>,
    pub replay: Option<ReplayStats>,
}

pub struct BenchServer {
    /// The TCP listener's address, if one listens.
    pub address: Option<SocketAddr>,
    /// The local socket, if one listens.
    pub socket: Option<PathBuf>,
    pub token: String,
    pub state_dir: PathBuf,
    pub project_id: Uuid,
    pub project_dir: PathBuf,
    /// The default timeline's name.
    pub timeline: String,
    /// The default timeline's thread.
    pub default_thread: Uuid,
    /// `(timeline, thread)` for every timeline thread.
    pub timeline_threads: Vec<(String, Uuid)>,
    /// The `--history-turns` threads.
    pub history_threads: Vec<Uuid>,
    /// The thread `--autoplay` prompts, when enabled.
    pub autoplay_thread: Option<Uuid>,
    shutdown: waku_core::ShutdownSignal,
    reports: Arc<Mutex<Vec<RunReport>>>,
    server: Option<JoinHandle<anyhow::Result<()>>>,
    watcher: Option<JoinHandle<()>>,
    autoplay: Option<JoinHandle<()>>,
}

impl BenchServer {
    pub fn start(options: BenchOptions) -> anyhow::Result<Self> {
        // The default timeline, and the thread list: a file joins as its
        // own thread, first.
        let (timeline, timeline_spec) = if BUILTIN_NAMES.contains(&options.timeline.as_str()) {
            (options.timeline.clone(), options.timeline.clone())
        } else {
            let path = std::path::absolute(&options.timeline)
                .with_context(|| format!("invalid timeline path {}", options.timeline))?;
            let custom = Timeline::resolve(&path.to_string_lossy())?;
            (custom.name, path.to_string_lossy().into_owned())
        };
        let mut names = BUILTIN_NAMES.map(str::to_owned).to_vec();
        if !names.contains(&timeline) {
            names.insert(0, timeline.clone());
        }

        prepare_state_dir(&options.state_dir)?;
        let state_dir = std::path::absolute(&options.state_dir)?;
        let project_dir = match options.project_dir.clone() {
            Some(path) => std::path::absolute(path)?,
            None => {
                let path = state_dir.join("bench-project");
                std::fs::create_dir_all(&path)?;
                std::fs::write(
                    path.join("README.md"),
                    "# Bench project\n\nCreated by waku-bench-daemon.\n",
                )?;
                path
            }
        };

        let bin_dir = state_dir.join("bin");
        let stats_path = state_dir.join(STATS_FILE);
        let fakes = install_fakes(
            &bin_dir,
            options.fakes_dir.as_deref(),
            &json!({
                "timeline": timeline_spec,
                "rate": options.rate.to_string(),
                "loops": options.loops.max(1),
                "stats": stats_path,
            }),
        )?;

        let settings = DaemonSettingsStore::open(state_dir.join("settings.json"))
            .context("could not open the bench daemon settings")?;
        let mut document = settings.get();
        for (provider, binary) in fakes {
            document
                .provider_binary_overrides
                .insert(provider, binary.display().to_string());
        }
        document.disabled_providers = ProviderKind::ALL
            .into_iter()
            .filter(|provider| !BENCH_PROVIDERS.contains(provider))
            .collect();
        settings.replace(document)?;
        let backend = Arc::new(WakuBackend::new(settings, state_dir.to_path_buf())?);
        waku_core::daemon::start_worktree_cleanup_scheduler(&backend)
            .context("could not start worktree cleanup")?;
        let host = DaemonHost::start(&backend, HostOptions::default())
            .context("could not start the orchestrator")?;

        let seeded = fixture::seed(
            host.orchestrator(),
            &FixtureOptions {
                project_path: project_dir.clone(),
                timelines: names,
                provider: options.provider,
                history_turns: options.history_turns,
                history_sessions: options.history_sessions,
            },
        )
        .context("could not seed the bench fixture")?;
        let default_thread = seeded
            .thread_of(&timeline)
            .expect("the default timeline has a thread");

        // Listeners before anything is reported, so a client that connects
        // right after never finds nothing listening.
        let tcp = match (&options.listen, &options.socket) {
            (Some(listen), _) => Some(listen.as_str()),
            (None, Some(_)) => None,
            (None, None) => Some("127.0.0.1:0"),
        }
        .map(|listen| {
            TcpListener::bind(listen).with_context(|| format!("could not listen on {listen}"))
        })
        .transpose()?;
        let address = tcp.as_ref().map(TcpListener::local_addr).transpose()?;
        let local = options
            .socket
            .as_deref()
            .map(waku_core::LocalListener::bind)
            .transpose()?;
        let socket = local.as_ref().map(|local| local.path().to_path_buf());

        let shutdown = waku_core::ShutdownSignal::new();
        let reports = Arc::new(Mutex::new(Vec::new()));
        let orchestration = host.orchestration();
        let watcher = {
            let orchestrator = host.orchestrator().clone();
            let feed = orchestrator.subscribe();
            let reports = reports.clone();
            let shutdown = shutdown.clone();
            let exit_after = options.exit_after;
            std::thread::Builder::new()
                .name("waku-bench-runs".into())
                .spawn(move || {
                    watch_runs(
                        &orchestrator,
                        feed,
                        StatsReader::new(stats_path),
                        &reports,
                        exit_after,
                        &shutdown,
                    );
                })?
        };
        let server = {
            let token = options.token.clone();
            let shutdown = shutdown.clone();
            let server_options = ServerOptions {
                allow_shutdown: options.allow_shutdown,
                orchestration: Some(orchestration.clone()),
                environment_id: Some(host.environment_id()),
                ..ServerOptions::default()
            };
            std::thread::Builder::new()
                .name("waku-bench-daemon-server".into())
                .spawn(move || {
                    let result = waku_core::serve_listeners(
                        waku_core::Listeners { tcp, local },
                        token,
                        backend,
                        shutdown.clone(),
                        server_options,
                    );
                    // Every exit path stops the run watcher and autoplay.
                    shutdown.trigger();
                    host.shutdown();
                    result
                })?
        };
        let autoplay_thread = options.autoplay.map(|_| default_thread);
        let autoplay = options
            .autoplay
            .map(|delay| {
                autoplay::start(
                    orchestration,
                    shutdown.clone(),
                    AutoplayOptions {
                        thread_id: default_thread,
                        prompt: timeline.clone(),
                        delay,
                    },
                )
            })
            .transpose()?;
        Ok(Self {
            address,
            socket,
            token: options.token,
            state_dir,
            project_id: seeded.project_id,
            project_dir,
            timeline,
            default_thread,
            timeline_threads: seeded.timeline_threads,
            history_threads: seeded.history_threads,
            autoplay_thread,
            shutdown,
            reports,
            server: Some(server),
            watcher: Some(watcher),
            autoplay,
        })
    }

    /// What a client passes to connect: `unix:<socket>` when a local socket
    /// listens, else the TCP address.
    pub fn client_address(&self) -> String {
        match (&self.socket, &self.address) {
            (Some(socket), _) => format!("unix:{}", socket.display()),
            (None, Some(address)) => address.to_string(),
            (None, None) => unreachable!("the bench daemon always listens"),
        }
    }

    pub fn shutdown_flag(&self) -> waku_core::ShutdownSignal {
        self.shutdown.clone()
    }

    /// Every run that ended so far, in the order they ended.
    pub fn reports(&self) -> Vec<RunReport> {
        self.reports.lock().clone()
    }

    /// Block until the server stops (signal, `daemon.shutdown` or
    /// `exit_after`), then return every run's report.
    pub fn wait(mut self) -> anyhow::Result<Vec<RunReport>> {
        let result = self.join();
        result?;
        Ok(self.reports())
    }

    pub fn stop(self) -> anyhow::Result<Vec<RunReport>> {
        self.shutdown.trigger();
        self.wait()
    }

    fn join(&mut self) -> anyhow::Result<()> {
        let result = match self.server.take() {
            Some(server) => server
                .join()
                .map_err(|_| anyhow::anyhow!("the bench daemon server thread panicked"))?,
            None => Ok(()),
        };
        self.shutdown.trigger();
        for thread in [self.watcher.take(), self.autoplay.take()]
            .into_iter()
            .flatten()
        {
            let _ = thread.join();
        }
        result
    }
}

impl Drop for BenchServer {
    fn drop(&mut self) {
        self.shutdown.trigger();
        let _ = self.join();
    }
}

/// Links `codex` and `claude` in `bin_dir` to the fakes, with `config` as
/// their `waku-bench.json`. Returns each provider's binary.
fn install_fakes(
    bin_dir: &Path,
    fakes_dir: Option<&Path>,
    config: &serde_json::Value,
) -> anyhow::Result<Vec<(ProviderKind, PathBuf)>> {
    let fakes_dir = match fakes_dir {
        Some(dir) => dir.to_path_buf(),
        None => std::env::current_exe()
            .context("could not locate waku-bench-daemon")?
            .parent()
            .context("waku-bench-daemon has no directory")?
            .to_path_buf(),
    };
    std::fs::create_dir_all(bin_dir)?;
    std::fs::write(
        bin_dir.join(CONFIG_FILE),
        serde_json::to_vec_pretty(config)?,
    )?;
    let mut binaries = Vec::new();
    for provider in BENCH_PROVIDERS {
        let fake = fakes_dir.join(format!(
            "fake-{}{}",
            provider.command(),
            std::env::consts::EXE_SUFFIX
        ));
        if !fake.is_file() {
            bail!(
                "{} is missing; build it with `cargo build -p waku-bench-daemon` (or pass --fakes DIR)",
                fake.display()
            );
        }
        let link = bin_dir.join(format!(
            "{}{}",
            provider.command(),
            std::env::consts::EXE_SUFFIX
        ));
        // Invoked through the link, a fake reads the config beside it.
        #[cfg(unix)]
        std::os::unix::fs::symlink(&fake, &link)
            .with_context(|| format!("could not link {}", link.display()))?;
        #[cfg(not(unix))]
        std::fs::copy(&fake, &link)
            .with_context(|| format!("could not copy {}", link.display()))?;
        binaries.push((provider, link));
    }
    Ok(binaries)
}

/// Reads the replays the fakes append to the stats file, in order.
struct StatsReader {
    path: PathBuf,
    reader: Option<BufReader<std::fs::File>>,
    pending: VecDeque<ReplayStats>,
}

impl StatsReader {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            reader: None,
            pending: VecDeque::new(),
        }
    }

    /// The oldest replay not handed out yet.
    fn next(&mut self) -> Option<ReplayStats> {
        if self.reader.is_none() {
            self.reader = std::fs::File::open(&self.path).ok().map(BufReader::new);
        }
        if let Some(reader) = &mut self.reader {
            let mut line = String::new();
            while reader.read_line(&mut line).is_ok_and(|read| read > 0) {
                if line.ends_with('\n') {
                    if let Some(stats) = serde_json::from_str(&line)
                        .ok()
                        .as_ref()
                        .and_then(ReplayStats::from_json)
                    {
                        self.pending.push_back(stats);
                    }
                    line.clear();
                }
            }
        }
        self.pending.pop_front()
    }
}

/// Follows the committed feed and reports every run that ends: one log line
/// each (the line `scripts/bench/harness.py` waits for) and an entry in
/// `reports`. Stops the daemon after `exit_after` replays.
fn watch_runs(
    orchestrator: &Orchestrator,
    mut feed: CommittedFeed,
    mut stats: StatsReader,
    reports: &Mutex<Vec<RunReport>>,
    exit_after: Option<usize>,
    shutdown: &waku_core::ShutdownSignal,
) {
    let mut ended = HashSet::new();
    let mut replays = 0;
    loop {
        let event = match feed.recv_timeout(Duration::from_millis(250)) {
            Ok(Some(event)) => event,
            Ok(None) if shutdown.is_triggered() => return,
            Ok(None) => continue,
            Err(FeedEnded::Overflowed) => {
                feed = orchestrator.subscribe();
                continue;
            }
            Err(FeedEnded::Closed) => return,
        };
        let DomainEvent::RunUpdated(run) = &event.event else {
            continue;
        };
        if !run.status.is_terminal() || !ended.insert(run.id) {
            continue;
        }
        let report = RunReport {
            thread_id: run.thread_id.0,
            run_id: run.id,
            status: run.status,
            run_ms: run_ms(run),
            replay: stats.next(),
        };
        log_report(&report);
        if report.replay.is_some() {
            replays += 1;
        }
        reports.lock().push(report);
        if exit_after.is_some_and(|limit| replays >= limit) {
            shutdown.trigger();
        }
    }
}

fn run_ms(run: &Run) -> Option<i64> {
    run.completed_at.map(|end| end - run.requested_at)
}

fn log_report(report: &RunReport) {
    let run = match report.run_ms {
        Some(ms) => format!("run {} after {ms} ms", report.status.as_str()),
        None => format!("run {}", report.status.as_str()),
    };
    match &report.replay {
        // `scripts/bench/harness.py` parses this line.
        Some(replay) => eprintln!(
            "waku-bench-daemon: {} replay {} · {} events in {:.2}s ({:.0}/s, max lag {:.1} ms) · {run}",
            replay.timeline,
            if replay.canceled {
                "canceled"
            } else {
                "finished"
            },
            replay.events,
            replay.elapsed.as_secs_f64(),
            replay.events_per_second(),
            replay.max_lag.as_secs_f64() * 1_000.0,
        ),
        None => eprintln!(
            "waku-bench-daemon: {} ended without a replay · {run}",
            report.run_id
        ),
    }
}

/// Start from an empty data directory every run, so every run renders the
/// same state. Refuses to wipe a directory the bench did not create.
fn prepare_state_dir(dir: &Path) -> anyhow::Result<()> {
    if dir.exists() {
        let owned = dir.join(STATE_MARKER).is_file();
        let empty = std::fs::read_dir(dir)?.next().is_none();
        if !owned && !empty {
            bail!(
                "refusing to reuse {}: it is not empty and was not created by waku-bench-daemon",
                dir.display()
            );
        }
        if owned {
            std::fs::remove_dir_all(dir)
                .with_context(|| format!("could not reset {}", dir.display()))?;
        }
    }
    std::fs::create_dir_all(dir).with_context(|| format!("could not create {}", dir.display()))?;
    std::fs::write(dir.join(STATE_MARKER), "")?;
    Ok(())
}

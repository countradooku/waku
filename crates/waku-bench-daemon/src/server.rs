//! Starting and stopping a bench daemon, for the CLI and for tests.

use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

use anyhow::{Context as _, bail};
use waku_core::model::ProviderKind;
use waku_core::persistence::StateStore;
use waku_core::{DaemonSettingsStore, ServerOptions};

use crate::backend::{BenchBackend, BenchSettings};
use crate::fixture;
use crate::replay::{Rate, ReplayStats};
use crate::timeline::Timeline;

/// Marks a state directory as owned by the bench, so it may be wiped.
const STATE_MARKER: &str = ".waku-bench-daemon";

pub struct BenchOptions {
    pub listen: String,
    pub token: String,
    /// Every timeline served; the fixture gets one session per entry.
    pub timelines: Vec<Arc<Timeline>>,
    pub default_timeline: String,
    pub rate: Rate,
    pub loops: u32,
    pub provider: ProviderKind,
    pub history_turns: usize,
    pub exit_after: Option<usize>,
    /// Throwaway daemon database and settings. Recreated on every start.
    pub state_dir: PathBuf,
    /// Project directory listed in the fixture; defaults to one inside
    /// `state_dir`.
    pub project_dir: Option<PathBuf>,
    /// Desktop-owned daemons accept a client's shutdown request; so does the
    /// bench, so quitting the app ends the run and prints the summary.
    pub allow_shutdown: bool,
}

pub struct BenchServer {
    pub address: SocketAddr,
    pub token: String,
    pub state_dir: PathBuf,
    shutdown: Arc<AtomicBool>,
    backend: Arc<BenchBackend>,
    thread: Option<JoinHandle<anyhow::Result<()>>>,
}

impl BenchServer {
    pub fn start(options: BenchOptions) -> anyhow::Result<Self> {
        if options.timelines.is_empty() {
            bail!("at least one timeline is required");
        }
        prepare_state_dir(&options.state_dir)?;
        let project_dir = match options.project_dir.clone() {
            Some(path) => path,
            None => {
                let path = options.state_dir.join("bench-project");
                std::fs::create_dir_all(&path)?;
                std::fs::write(
                    path.join("README.md"),
                    "# Bench project\n\nCreated by waku-bench-daemon.\n",
                )?;
                path
            }
        };
        let names = options
            .timelines
            .iter()
            .map(|timeline| timeline.name.clone())
            .collect::<Vec<_>>();
        let fixture = fixture::build(project_dir, &names, options.provider, options.history_turns);
        let state_path = options.state_dir.join("app.db");
        fixture::seed(&state_path, &fixture)?;

        let settings = DaemonSettingsStore::open(options.state_dir.join("settings.json"))
            .context("could not open bench daemon settings")?;
        let inner = waku_core::daemon::WakuBackend::new(settings, StateStore::daemon(state_path))?;
        let timelines = options
            .timelines
            .iter()
            .map(|timeline| (timeline.name.clone(), timeline.clone()))
            .collect::<HashMap<_, _>>();
        if !timelines.contains_key(&options.default_timeline) {
            bail!(
                "default timeline {:?} is not loaded",
                options.default_timeline
            );
        }
        let bin_dir = options.state_dir.join("bin");
        let shutdown = Arc::new(AtomicBool::new(false));
        let backend = Arc::new(BenchBackend::new(
            inner,
            BenchSettings {
                timelines,
                default_timeline: options.default_timeline,
                session_timelines: fixture.bindings.into_iter().collect(),
                rate: options.rate,
                loops: options.loops,
                exit_after: options.exit_after,
                binary_dir: bin_dir,
            },
            shutdown.clone(),
        ));

        let listener = TcpListener::bind(&options.listen)
            .with_context(|| format!("could not listen on {}", options.listen))?;
        let address = listener.local_addr()?;
        let server_backend: Arc<dyn waku_core::Backend> = backend.clone();
        let server_shutdown = shutdown.clone();
        let token = options.token.clone();
        let allow_shutdown = options.allow_shutdown;
        let thread = std::thread::Builder::new()
            .name("waku-bench-daemon-server".into())
            .spawn(move || {
                waku_core::serve(
                    listener,
                    token,
                    server_backend,
                    server_shutdown,
                    ServerOptions {
                        allowed_origins: Default::default(),
                        allow_shutdown,
                    },
                )
            })?;
        Ok(Self {
            address,
            token: options.token,
            state_dir: options.state_dir,
            shutdown,
            backend,
            thread: Some(thread),
        })
    }

    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        self.shutdown.clone()
    }

    pub fn stats(&self) -> Vec<ReplayStats> {
        self.backend.stats()
    }

    /// Block until the server stops (signal, client shutdown or
    /// `exit_after`), then return every replay's statistics.
    pub fn wait(mut self) -> anyhow::Result<Vec<ReplayStats>> {
        let result = match self.thread.take() {
            Some(thread) => thread
                .join()
                .map_err(|_| anyhow::anyhow!("bench daemon server thread panicked"))?,
            None => Ok(()),
        };
        result?;
        Ok(self.backend.stats())
    }

    pub fn stop(self) -> anyhow::Result<Vec<ReplayStats>> {
        self.shutdown.store(true, Ordering::Release);
        self.wait()
    }
}

impl Drop for BenchServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Start from an empty database every run, so every run renders the same
/// state. Refuses to wipe a directory the bench did not create.
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

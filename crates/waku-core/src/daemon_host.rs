//! The daemon's orchestration host (#55): the orchestrator as the daemon's
//! single source of truth, and everything that runs beside it.
//!
//! [`DaemonHost::start`] runs RFC 0001's startup sequence before the daemon
//! accepts a connection:
//!
//! 1. open `state.sqlite` in the data directory (migrations, projection
//!    version check);
//! 2. `EffectWorkerPool::reconcile_after_process_loss`: process-bound
//!    effects of the previous process are cancelled, replay-safe ones
//!    requeued;
//! 3. the provider session manager with every ported `DriverV2` registered
//!    from the daemon settings ([`provider_instances`]), on the shared driver
//!    runtime, feeding the output hub and the delta bus ([`LiveStreams`]) the
//!    v8 endpoint reads;
//! 4. the effect worker pool with `DefaultExecutor`: provider effects go to
//!    the session manager, the rest to `core_ports` (the managed worktree
//!    folder and cleanup rules from the storage settings, the daemon's
//!    terminals, the attachment store);
//! 5. the lifecycle service (auto-settle, snooze wake);
//! 6. the blob sweeper ([`crate::blob_sweep`]), which keeps what the
//!    orchestrator, the drafts and the legacy sessions reference, and a
//!    watcher that asks it for a sweep when a thread is deleted or rolled
//!    back.
//!
//! [`DaemonHost::shutdown`] stops claiming effects and waits up to
//! [`SHUTDOWN_GRACE`] for running ones, stops the lifecycle service and the
//! blob sweeper, closes
//! every provider session, then stops the orchestrator. Effects still
//! running are left `running` for the next start's reconciliation (RFC 0001:
//! shutdown is a crash, one recovery path).
//!
//! The environment id clients see (RFC 0003 `EnvironmentInfo.id`) is kept in
//! [`ENVIRONMENT_ID_FILE`] so it survives restarts.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use uuid::Uuid;
use waku_orchestrator::lifecycle::{AutoSettleSettings, LifecycleService, NoPullRequests};
use waku_orchestrator::streams::LiveStreams;
use waku_orchestrator::{
    Clock, DefaultExecutor, Dependencies, EffectWorkerPool, Orchestrator, OrchestratorConfig,
    SystemClock, UuidV7Ids, WorkerConfig,
};
use waku_protocol::orchestration::DriverKind;

use crate::Orchestration;
use crate::attachments::AttachmentStore;
use crate::blob_sweep::{BlobSweeper, SweepTimes, orchestrator_references, watch_releases};
use crate::daemon::WakuBackend;
use crate::driver::v2::manager::{
    DriverRegistry, ManagerDeps, ManagerExecutor, ProviderSessionManager, RegisteredInstance,
    SessionManagerConfig,
};
use crate::driver::v2::{
    DriverV2, InstanceConfig, NullRawLog, ProcessTransportFactory, UuidIdAllocator,
};
use crate::model::ProviderKind;
use crate::orchestrator_ports::{
    CodexTitleBackend, CoreCleanupPort, GitCheckpointPort, GitWorkspacePort, ProviderTitlePort,
    core_ports,
};
use crate::settings::DaemonSettings;

/// The orchestrator's database, in the daemon's data directory.
pub const STATE_DATABASE: &str = "state.sqlite";
/// Holds the daemon's environment id.
pub const ENVIRONMENT_ID_FILE: &str = "environment-id";
/// How long shutdown waits for running effects.
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
/// How often idle provider sessions are released.
const IDLE_REAP_PERIOD: Duration = Duration::from_secs(60);
/// Settings key of the per-provider home directories (`CODEX_HOME`,
/// `CLAUDE_CONFIG_DIR`, …): `{"codex": "/path", …}`. It lives in the
/// settings document's open part, which every client round-trips, until
/// clients edit it.
pub const PROVIDER_HOMES_KEY: &str = "provider_homes";

/// What [`DaemonHost::start`] can be told besides the backend.
#[derive(Clone, Debug, Default)]
pub struct HostOptions {
    /// `None`: the backend's data directory.
    pub data_dir: Option<PathBuf>,
    pub workers: WorkerConfig,
    pub blob_sweeps: SweepTimes,
}

/// The running orchestrator, its effect workers, provider sessions and
/// lifecycle service.
pub struct DaemonHost {
    orchestration: Arc<Orchestration>,
    manager: Arc<ProviderSessionManager>,
    registry: DriverRegistry,
    pool: Option<EffectWorkerPool>,
    lifecycle: Option<LifecycleService>,
    sweeper: Option<BlobSweeper>,
    release_watch: Option<std::thread::JoinHandle<()>>,
    environment_id: Uuid,
}

impl DaemonHost {
    /// Opens the orchestrator for `backend`'s data directory and starts
    /// everything that runs beside it (see the module documentation).
    pub fn start(backend: &Arc<WakuBackend>, options: HostOptions) -> anyhow::Result<Self> {
        let data_dir = options.data_dir.unwrap_or_else(|| backend.data_dir());
        std::fs::create_dir_all(&data_dir)
            .with_context(|| format!("could not create {}", data_dir.display()))?;
        let environment_id = environment_id(&data_dir)?;
        // Provider CLIs run with the login shell's environment; load it off
        // the startup path.
        std::thread::Builder::new()
            .name("waku-shell-environment".into())
            .spawn(crate::daemon::ensure_shell_environment)?;

        let registry = DriverRegistry::new();
        registry.replace(provider_instances(&backend.daemon_settings()));
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let database = data_dir.join(STATE_DATABASE);
        let orchestrator = Orchestrator::open(
            OrchestratorConfig::new(database.clone()),
            Dependencies {
                clock: clock.clone(),
                ids: Box::new(UuidV7Ids),
                capabilities: Arc::new(registry.clone()),
            },
        )
        .with_context(|| format!("could not open {}", database.display()))?;
        let reconciled = EffectWorkerPool::reconcile_after_process_loss(&orchestrator)
            .context("could not reconcile effects after a restart")?;
        if !reconciled.cancelled.is_empty() || !reconciled.requeued.is_empty() {
            eprintln!(
                "waku-daemon recovered effects: {} cancelled, {} requeued",
                reconciled.cancelled.len(),
                reconciled.requeued.len()
            );
        }

        let live = LiveStreams::default();
        let manager = ProviderSessionManager::new(
            ManagerDeps {
                orchestrator: orchestrator.clone(),
                registry: registry.clone(),
                transports: Arc::new(ProcessTransportFactory),
                ids: Arc::new(UuidIdAllocator),
                clock,
                output: live.output.clone(),
                deltas: Some(live.deltas.clone()),
                raw_log: Arc::new(NullRawLog),
                checkpoints: Some(Arc::new(GitCheckpointPort)),
                runtime: crate::driver::v2::bridge::runtime(),
            },
            SessionManagerConfig::default(),
        );
        manager.spawn_idle_reaper(IDLE_REAP_PERIOD);

        let ports = core_ports(
            workspace_port(backend),
            title_port(&registry, data_dir.clone()),
            CoreCleanupPort::new(
                backend.terminal_map(),
                AttachmentStore::new(data_dir.join("attachments")),
            ),
        );
        let executor = Arc::new(DefaultExecutor {
            provider: Arc::new(ManagerExecutor(manager.clone())),
            ports,
        });
        let pool = EffectWorkerPool::start(orchestrator.clone(), executor, options.workers);
        let lifecycle = LifecycleService::start(
            orchestrator.clone(),
            AutoSettleSettings::default(),
            Arc::new(NoPullRequests),
        );

        let swept = orchestrator.clone();
        let legacy = backend.asset_sweep();
        let sweeper =
            BlobSweeper::start(options.blob_sweeps, move || {
                match orchestrator_references(&swept) {
                    Ok(references) => legacy.run(&references),
                    Err(error) => eprintln!("waku-daemon skipped a blob sweep: {error:#}"),
                }
            });
        backend.set_blob_sweeps(sweeper.handle());
        let watched = orchestrator.clone();
        let sweeps = sweeper.handle();
        let release_watch = std::thread::Builder::new()
            .name("waku-blob-release-watch".into())
            .spawn(move || watch_releases(&watched, &sweeps))?;

        let observed = registry.clone();
        backend.on_settings_changed(Box::new(move |settings| {
            observed.replace(provider_instances(settings));
        }));

        Ok(Self {
            orchestration: Orchestration::new(orchestrator, live),
            manager,
            registry,
            pool: Some(pool),
            lifecycle: Some(lifecycle),
            sweeper: Some(sweeper),
            release_watch: Some(release_watch),
            environment_id,
        })
    }

    /// What the v8 endpoint serves.
    pub fn orchestration(&self) -> Arc<Orchestration> {
        self.orchestration.clone()
    }

    pub fn orchestrator(&self) -> &Arc<Orchestrator> {
        &self.orchestration.orchestrator
    }

    pub fn manager(&self) -> &Arc<ProviderSessionManager> {
        &self.manager
    }

    pub fn registry(&self) -> &DriverRegistry {
        &self.registry
    }

    /// The persistent environment id.
    pub fn environment_id(&self) -> Uuid {
        self.environment_id
    }

    /// Stops everything in order (see the module documentation).
    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        if let Some(pool) = self.pool.take() {
            pool.shutdown(SHUTDOWN_GRACE);
        }
        if let Some(lifecycle) = self.lifecycle.take() {
            lifecycle.shutdown();
        }
        if let Some(sweeper) = self.sweeper.take() {
            sweeper.shutdown();
        }
        // Not on a runtime thread: the daemon's main thread or a test's.
        let manager = self.manager.clone();
        let runtime = crate::driver::v2::bridge::runtime();
        let _ = std::thread::Builder::new()
            .name("waku-session-shutdown".into())
            .spawn(move || runtime.block_on(manager.shutdown()))
            .map(|thread| thread.join());
        self.orchestration.shutdown();
        // The feed closed with the orchestrator.
        if let Some(watch) = self.release_watch.take() {
            let _ = watch.join();
        }
    }
}

impl Drop for DaemonHost {
    fn drop(&mut self) {
        if self.pool.is_some() {
            self.stop();
        }
    }
}

/// The environment id in `data_dir`, created on first use.
pub fn environment_id(data_dir: &Path) -> anyhow::Result<Uuid> {
    let path = data_dir.join(ENVIRONMENT_ID_FILE);
    match std::fs::read_to_string(&path) {
        Ok(text) => {
            if let Ok(id) = Uuid::parse_str(text.trim()) {
                return Ok(id);
            }
            eprintln!(
                "waku-daemon replaces the unreadable environment id in {}",
                path.display()
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| format!("could not read {}", path.display()));
        }
    }
    let id = Uuid::new_v4();
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, format!("{id}\n"))
        .and_then(|()| std::fs::rename(&temporary, &path))
        .with_context(|| format!("could not write {}", path.display()))?;
    Ok(id)
}

/// The provider instances the settings ask for: one per provider with a
/// `DriverV2` port, minus the disabled ones. The instance id is the
/// provider's id (`codex`, `claude`, `cursor`, …; RFC 0001: Phase 2
/// instances are the driver tag).
///
/// The binary is the settings' override when one is set, else the
/// provider's command, which the spawn resolves on the login shell's search
/// path. The provider home comes from the settings' [`PROVIDER_HOMES_KEY`],
/// else from the daemon's environment (`CODEX_HOME`, `CLAUDE_CONFIG_DIR`).
pub fn provider_instances(settings: &DaemonSettings) -> Vec<RegisteredInstance> {
    let homes = settings
        .extra
        .get(PROVIDER_HOMES_KEY)
        .and_then(|value| {
            serde_json::from_value::<std::collections::HashMap<ProviderKind, PathBuf>>(
                value.clone(),
            )
            .ok()
        })
        .unwrap_or_default();
    ProviderKind::ALL
        .into_iter()
        .filter(|provider| !settings.disabled_providers.contains(provider))
        .filter_map(|provider| {
            let driver = driver_for(provider)?;
            let mut config = InstanceConfig::new(provider.id(), DriverKind::from(provider));
            config.binary = settings
                .provider_binary_overrides
                .get(&provider)
                .map(|spec| {
                    crate::command_env::resolve_binary_override(spec)
                        .unwrap_or_else(|| PathBuf::from(spec.trim()))
                })
                .unwrap_or_else(|| PathBuf::from(provider.command()));
            config.provider_home = homes
                .get(&provider)
                .cloned()
                .or_else(|| home_from_environment(provider));
            config.computer_use_enabled =
                crate::computer_use::resolve_enabled(settings.computer_use_enabled);
            Some(RegisteredInstance { driver, config })
        })
        .collect()
}

/// The `DriverV2` port of `provider`; `None` for providers still served
/// only through the legacy protocol.
fn driver_for(provider: ProviderKind) -> Option<Arc<dyn DriverV2>> {
    Some(match provider {
        ProviderKind::Codex => Arc::new(crate::driver::codex::CodexDriver::new()),
        ProviderKind::Claude => Arc::new(crate::driver::claude::ClaudeDriver),
        ProviderKind::Cursor | ProviderKind::Fx | ProviderKind::Grok | ProviderKind::Kimi => {
            Arc::new(crate::driver::acp::AcpDriver::new())
        }
        ProviderKind::OpenCode => Arc::new(crate::driver::opencode::OpenCodeDriver),
        ProviderKind::DeepSeek => Arc::new(crate::driver::deepseek::DeepSeekDriver),
        // TODO(#50, #51): register Pi, Oh My Pi and Amp here once their
        // `DriverV2` ports land.
        ProviderKind::Pi | ProviderKind::OhMyPi | ProviderKind::Amp => return None,
    })
}

fn home_from_environment(provider: ProviderKind) -> Option<PathBuf> {
    let variable = match provider {
        ProviderKind::Codex => "CODEX_HOME",
        ProviderKind::Claude => "CLAUDE_CONFIG_DIR",
        _ => return None,
    };
    std::env::var_os(variable)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

/// Task worktrees in the managed folder of the storage settings, removed
/// under the user's cleanup rules, both read at use.
fn workspace_port(backend: &Arc<WakuBackend>) -> GitWorkspacePort {
    let folder = Arc::downgrade(backend);
    let rules = Arc::downgrade(backend);
    GitWorkspacePort::new(
        move || {
            let backend = folder
                .upgrade()
                .ok_or_else(|| anyhow::anyhow!("the daemon is shutting down"))?;
            crate::worktree_cleanup::worktrees_directory(&backend.daemon_settings().storage)
        },
        move || {
            rules
                .upgrade()
                .map(|backend| backend.daemon_settings().storage.worktree_cleanup)
                .unwrap_or_default()
        },
    )
}

/// Codex threads get Codex's own titles, through the configured Codex
/// binary (a missing CLI is "no title").
fn title_port(registry: &DriverRegistry, cwd: PathBuf) -> ProviderTitlePort {
    let registry = registry.clone();
    let codex = waku_protocol::orchestration::ProviderInstanceId::new(ProviderKind::Codex.id());
    ProviderTitlePort::new().route(
        DriverKind::Codex,
        Arc::new(CodexTitleBackend::with_binary(cwd, move || {
            let binary = registry.get(&codex)?.config.binary;
            crate::command_env::find_executable(&binary.to_string_lossy())
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instances_follow_the_settings() {
        let mut settings = DaemonSettings::default();
        settings.disabled_providers.push(ProviderKind::Grok);
        settings
            .provider_binary_overrides
            .insert(ProviderKind::Codex, "/opt/fake/codex".into());
        settings.extra.insert(
            PROVIDER_HOMES_KEY.into(),
            serde_json::json!({"claude": "/homes/claude"}),
        );
        let instances = provider_instances(&settings);
        let ids = instances
            .iter()
            .map(|instance| instance.config.instance_id.as_str().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            [
                "claude", "codex", "cursor", "deepseek", "fx", "opencode", "kimi"
            ]
        );
        let get = |id: &str| {
            instances
                .iter()
                .find(|instance| instance.config.instance_id.as_str() == id)
                .unwrap()
        };
        assert_eq!(get("codex").config.binary, PathBuf::from("/opt/fake/codex"));
        assert_eq!(get("codex").config.provider, DriverKind::Codex);
        assert_eq!(get("cursor").config.binary, PathBuf::from("cursor-agent"));
        assert_eq!(get("kimi").config.provider, DriverKind::Kimi);
        assert_eq!(
            get("claude").config.provider_home,
            Some(PathBuf::from("/homes/claude"))
        );
    }

    #[test]
    fn the_environment_id_survives_a_restart() {
        let root = tempfile::tempdir().unwrap();
        let first = environment_id(root.path()).unwrap();
        assert_eq!(environment_id(root.path()).unwrap(), first);
        std::fs::write(root.path().join(ENVIRONMENT_ID_FILE), "garbage").unwrap();
        let replaced = environment_id(root.path()).unwrap();
        assert_ne!(replaced, first);
        assert_eq!(environment_id(root.path()).unwrap(), replaced);
    }
}

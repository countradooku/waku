//! `ReplayHarness`: the real orchestrator, `EventSink`, projections,
//! effect workers, checkpoint capture and `ProviderSessionManager` around
//! one driver, with only the transport replaced (RFC 0002 "Harness API").
//!
//! Everything that could vary between runs is pinned: the orchestrator's
//! clock is a `ManualClock` starting at 2026-01-01T00:00:00Z that only moves
//! when a step (or a transcript's `afterMs`) moves it, app ids come from
//! counters (`SequentialIds`, `SeqIdAllocator`), and native ids bind through
//! UUIDv5. Every run ends by checking that rebuilding the projections from
//! the event log equals the live projections.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde::Deserialize;
use tempfile::TempDir;
use uuid::Uuid;
use waku_orchestrator::fakes::FakePorts;
use waku_orchestrator::{
    DefaultExecutor, DispatchError, EffectWorkerPool, ManualClock, Orchestrator,
    OrchestratorConfig, SequentialIds, ThreadProjection, WorkerConfig,
};
use waku_protocol::orchestration::*;

use super::transport::seed_home;
use super::{ReplayError, ReplayProfile, ReplayTranscript, ReplayTransportFactory};
use crate::driver::v2::manager::{
    DriverRegistry, ForkResult, ManagerDeps, ManagerExecutor, ProviderSessionManager,
    SessionManagerConfig,
};
use crate::driver::v2::{
    AdapterEvent, DriverV2, InstanceConfig, NullOutput, NullRawLog, SeqIdAllocator,
    TransportFactory,
};

/// 2026-01-01T00:00:00Z.
pub const REPLAY_EPOCH_MS: Timestamp = 1_767_225_600_000;

/// A scenario's provider-neutral steps (`<scenario>/input.json`).
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScenarioInput {
    /// Merged into the instance's `extra` (driver profile switches).
    #[serde(default)]
    pub instance: Option<serde_json::Value>,
    /// The main thread's runtime mode (full access when absent).
    #[serde(default)]
    pub runtime_mode: Option<RuntimeMode>,
    pub steps: Vec<ScenarioStep>,
}

impl ScenarioInput {
    /// `<dir>/input.<provider>.json` when a provider needs its own steps
    /// (native option ids, prompts its recording used), else the shared
    /// `<dir>/input.json`.
    pub fn load(dir: &std::path::Path, provider: &str) -> Result<Self, ReplayError> {
        let own = dir.join(format!("input.{provider}.json"));
        let path = if own.exists() {
            own
        } else {
            dir.join("input.json")
        };
        let text = std::fs::read_to_string(&path)
            .map_err(|error| ReplayError::Io(format!("{}: {error}", path.display())))?;
        serde_json::from_str(&text)
            .map_err(|error| ReplayError::Harness(format!("{}: {error}", path.display())))
    }
}

/// A model-selection change.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SelectionInput {
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum ScenarioStep {
    /// `mode`: `auto` (default), `queue`, `steer`, `restart`.
    Message {
        text: String,
        #[serde(default)]
        thread: Option<String>,
        #[serde(default)]
        mode: Option<String>,
    },
    /// `until`: `idle`, `running`, `turn_terminal`, `request_pending`,
    /// `tool_started`, or `ms:<n>` (moves the clock).
    Wait {
        until: String,
        #[serde(default)]
        thread: Option<String>,
    },
    Steer {
        text: String,
        #[serde(default)]
        thread: Option<String>,
    },
    Interrupt {
        #[serde(default)]
        thread: Option<String>,
    },
    Respond {
        request: String,
        option: String,
        #[serde(default)]
        thread: Option<String>,
    },
    Answer {
        request: String,
        answers: Vec<UserInputAnswer>,
        #[serde(default)]
        thread: Option<String>,
    },
    /// Rolls back the last `runs` runs to the checkpoint before them.
    Rollback {
        runs: u32,
        #[serde(default)]
        restore_files: bool,
        #[serde(default)]
        thread: Option<String>,
    },
    /// Forks into a new thread named `as`, after run `atRun` (or the tip).
    Fork {
        #[serde(default)]
        at_run: Option<u32>,
        #[serde(rename = "as")]
        name: String,
        #[serde(default)]
        thread: Option<String>,
    },
    Select {
        selection: SelectionInput,
        #[serde(default)]
        thread: Option<String>,
    },
    Compact {
        #[serde(default)]
        thread: Option<String>,
    },
    /// Releases sessions idle past the manager's limits.
    ReleaseIdle,
    /// Unloads a thread's session.
    Unload {
        #[serde(default)]
        thread: Option<String>,
    },
}

pub struct ScenarioResult {
    /// The main thread, after rebuild-from-log equality was checked.
    pub projection: ThreadProjection,
    /// Every thread by scenario name (`main`, fork names).
    pub threads: BTreeMap<String, ThreadProjection>,
    pub events: Vec<DomainEvent>,
    pub adapter_events: Vec<AdapterEvent>,
    pub checkpoints: Vec<Checkpoint>,
    pub capabilities: Capabilities,
    pub forks: Vec<ForkResult>,
    /// Calls the fake checkpoint port saw (`capture <ref>`, `restore <ref>`).
    pub checkpoint_calls: Vec<String>,
    /// Threads whose sessions `release_idle` steps released.
    pub released: Vec<ThreadId>,
    /// The harness workspace (`<workspace>` in transcripts).
    pub workspace: PathBuf,
}

pub struct ReplayHarness {
    runtime: tokio::runtime::Runtime,
    _dir: TempDir,
    pub workspace: PathBuf,
    pub home: PathBuf,
    clock: Arc<ManualClock>,
    orchestrator: Arc<Orchestrator>,
    manager: Arc<ProviderSessionManager>,
    pool: Option<EffectWorkerPool>,
    ports: FakePorts,
    instance: InstanceConfig,
    project: ProjectId,
    threads: BTreeMap<String, ThreadId>,
    adapter_events: Arc<Mutex<Vec<AdapterEvent>>>,
    replay: Option<Arc<ReplayTransportFactory>>,
    counter: u128,
    released: Vec<ThreadId>,
}

fn harness_error(error: impl std::fmt::Display) -> ReplayError {
    ReplayError::Harness(error.to_string())
}

/// Where a harness's provider frames go.
pub enum Transports {
    /// A transcript to replay, plus the fixture directory for a home seed.
    Replay {
        transcript: ReplayTranscript,
        fixture_dir: Option<PathBuf>,
        profile: ReplayProfile,
    },
    /// Live transports (the recorder wraps the real ones).
    Live(Arc<dyn TransportFactory>),
}

impl ReplayHarness {
    /// A harness over a transcript.
    pub fn replay(
        driver: Arc<dyn DriverV2>,
        instance: InstanceConfig,
        transcript: ReplayTranscript,
        fixture_dir: Option<PathBuf>,
        profile: ReplayProfile,
    ) -> Result<Self, ReplayError> {
        Self::new(
            driver,
            instance,
            Transports::Replay {
                transcript,
                fixture_dir,
                profile,
            },
        )
    }

    pub fn new(
        driver: Arc<dyn DriverV2>,
        instance: InstanceConfig,
        transports: Transports,
    ) -> Result<Self, ReplayError> {
        let dir = tempfile::tempdir().map_err(harness_error)?;
        // Canonical paths: drivers see symlink-free cwds (macOS /private/var).
        let root = dir.path().canonicalize().map_err(harness_error)?;
        let workspace = root.join("workspace");
        let home = root.join("home");
        std::fs::create_dir_all(&workspace).map_err(harness_error)?;
        std::fs::create_dir_all(&home).map_err(harness_error)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(harness_error)?;
        let clock = Arc::new(ManualClock::new(REPLAY_EPOCH_MS));
        let (factory, replay): (Arc<dyn TransportFactory>, _) = match transports {
            Transports::Replay {
                transcript,
                fixture_dir,
                profile,
            } => {
                if let Some(fixture_dir) = &fixture_dir {
                    seed_home(fixture_dir, &transcript.header.provider, &home)
                        .map_err(harness_error)?;
                }
                let replay = ReplayTransportFactory::new(
                    transcript,
                    workspace.clone(),
                    home.clone(),
                    profile,
                );
                let advance_clock = clock.clone();
                replay.set_clock_advance(Arc::new(move |ms| {
                    advance_clock.advance(ms as i64);
                }));
                (
                    Arc::new(replay.clone()) as Arc<dyn TransportFactory>,
                    Some(replay),
                )
            }
            Transports::Live(factory) => (factory, None),
        };
        let mut registry = DriverRegistry::new();
        registry.register(driver, instance.clone());
        let mut config = OrchestratorConfig::new(root.join("state.sqlite"));
        config.read_connections = 2;
        let orchestrator = Orchestrator::open(
            config,
            waku_orchestrator::Dependencies {
                clock: clock.clone(),
                ids: Box::new(SequentialIds::new(0x5eed)),
                capabilities: Arc::new(registry.clone()),
            },
        )
        .map_err(harness_error)?;
        let ports = FakePorts::default();
        let manager = ProviderSessionManager::new(
            ManagerDeps {
                orchestrator: orchestrator.clone(),
                registry,
                transports: factory,
                ids: Arc::new(SeqIdAllocator::new()),
                clock: clock.clone(),
                output: Arc::new(NullOutput),
                raw_log: Arc::new(NullRawLog),
                checkpoints: Some(ports.checkpoints.clone()),
                runtime: runtime.handle().clone(),
            },
            SessionManagerConfig::default(),
        );
        let executor = Arc::new(DefaultExecutor {
            provider: Arc::new(ManagerExecutor(manager.clone())),
            ports: ports.ports(),
        });
        let pool = EffectWorkerPool::start(orchestrator.clone(), executor, WorkerConfig::default());
        let adapter_events = Arc::new(Mutex::new(Vec::new()));
        let mut live = manager.subscribe();
        let sink = adapter_events.clone();
        runtime.spawn(async move {
            while let Ok(event) = live.recv().await {
                sink.lock().push(event.event);
            }
        });
        let mut harness = Self {
            runtime,
            _dir: dir,
            workspace,
            home,
            clock,
            orchestrator,
            manager,
            pool: Some(pool),
            ports,
            instance,
            project: ProjectId(Uuid::nil()),
            threads: BTreeMap::new(),
            adapter_events,
            replay,
            counter: 0,
            released: Vec::new(),
        };
        harness.project = ProjectId(harness.next_id(0xa));
        let project = harness.project;
        let workspace = harness.workspace.clone();
        harness.dispatch(Command::ProjectCreate {
            project_id: project,
            name: "Replay".to_owned(),
            path: workspace,
            order_key: None,
        })?;
        harness.create_thread("main")?;
        Ok(harness)
    }

    pub fn manager(&self) -> &Arc<ProviderSessionManager> {
        &self.manager
    }

    pub fn orchestrator(&self) -> &Arc<Orchestrator> {
        &self.orchestrator
    }

    pub fn clock(&self) -> &Arc<ManualClock> {
        &self.clock
    }

    pub fn thread_id(&self, name: &str) -> Option<ThreadId> {
        self.threads.get(name).copied()
    }

    fn next_id(&mut self, kind: u128) -> Uuid {
        self.counter += 1;
        Uuid::from_u128((kind << 96) | self.counter)
    }

    fn dispatch(&mut self, command: Command) -> Result<(), ReplayError> {
        self.clock.advance(1);
        let command_id =
            CommandId::new(format!("replay:{}", self.next_id(0xc))).expect("valid command id");
        let kind = command.command_type();
        self.orchestrator
            .dispatch(CommandEnvelope {
                command_id,
                issued_by: Actor::User,
                source: CreationSource::Desktop,
                command,
            })
            .map(|_| ())
            .map_err(|error: DispatchError| ReplayError::Harness(format!("{kind}: {error}")))
    }

    fn create_thread(&mut self, name: &str) -> Result<ThreadId, ReplayError> {
        let thread_id = ThreadId(self.next_id(0xb));
        self.dispatch(Command::ThreadCreate {
            thread_id,
            project_id: self.project,
            title: Some(name.to_owned()),
            model_selection: ModelSelection::new(self.instance.instance_id.as_str()),
            runtime_mode: RuntimeMode::FullAccess,
            interaction_mode: InteractionMode::Default,
            workspace: WorkspaceStrategy::Root { branch: None },
            imported_native_thread: None,
        })?;
        self.threads.insert(name.to_owned(), thread_id);
        Ok(thread_id)
    }

    fn thread(&self, name: &Option<String>) -> Result<ThreadId, ReplayError> {
        let name = name.as_deref().unwrap_or("main");
        self.threads
            .get(name)
            .copied()
            .ok_or_else(|| ReplayError::Harness(format!("no thread named {name}")))
    }

    fn projection(&self, thread_id: ThreadId) -> Result<Arc<ThreadProjection>, ReplayError> {
        self.orchestrator
            .thread(thread_id)
            .map_err(|error| ReplayError::Harness(error.to_string()))
    }

    fn active_run(&self, thread_id: ThreadId) -> Result<RunId, ReplayError> {
        self.projection(thread_id)?
            .active_run()
            .map(|run| run.id)
            .ok_or_else(|| ReplayError::Harness("no active run".to_owned()))
    }

    fn unsettled_effects(&self) -> i64 {
        self.orchestrator.reader().with(|connection| {
            connection
                .query_row(
                    "SELECT COUNT(*) FROM effect_outbox WHERE status IN ('pending', 'running')",
                    [],
                    |row| row.get(0),
                )
                .unwrap_or(0)
        })
    }

    fn replay_failed(&self) -> Result<(), ReplayError> {
        match self.replay.as_ref().and_then(|replay| replay.error()) {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Waits until nothing moves: no unsettled effect, no event in flight,
    /// no new commit and no replay progress for several polls.
    pub fn settle(&self) -> Result<(), ReplayError> {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut last = None;
        let mut stable = 0;
        loop {
            self.replay_failed()?;
            let snapshot = (
                self.orchestrator.last_sequence(),
                self.unsettled_effects(),
                self.manager.is_idle(),
                self.replay.as_ref().map_or(0, |replay| replay.progress()),
                self.adapter_events.lock().len(),
            );
            let quiet = snapshot.1 == 0 && snapshot.2;
            if quiet && last == Some(snapshot) {
                stable += 1;
                if stable >= 6 {
                    return Ok(());
                }
            } else {
                stable = 0;
            }
            last = Some(snapshot);
            if Instant::now() > deadline {
                return Err(ReplayError::Harness(format!(
                    "the scenario did not settle (unsettled effects: {}, manager idle: {})",
                    snapshot.1, snapshot.2
                )));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn wait_until(&self, thread_id: ThreadId, until: &str) -> Result<(), ReplayError> {
        if let Some(ms) = until.strip_prefix("ms:") {
            let ms = ms
                .parse::<i64>()
                .map_err(|_| ReplayError::Harness(format!("bad wait {until}")))?;
            self.clock.advance(ms);
            return self.settle();
        }
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            self.replay_failed()?;
            let projection = self.projection(thread_id)?;
            let done = match until {
                "idle" => true,
                "running" => projection
                    .active_run()
                    .is_some_and(|run| run.status == RunStatus::Running),
                "turn_terminal" => projection.active_run().is_none(),
                "request_pending" => projection
                    .runtime_requests
                    .values()
                    .any(|request| request.status == RequestStatus::Pending),
                "tool_started" => projection
                    .hot_items
                    .values()
                    .any(|item| matches!(item.kind, TurnItemKind::CommandExecution { .. })),
                other => return Err(ReplayError::Harness(format!("unknown wait {other}"))),
            };
            if done {
                return self.settle();
            }
            if Instant::now() > deadline {
                return Err(ReplayError::Harness(format!(
                    "timed out waiting for {until}"
                )));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn step(
        &mut self,
        step: &ScenarioStep,
        forks: &mut Vec<ForkResult>,
    ) -> Result<(), ReplayError> {
        match step {
            ScenarioStep::Message { text, thread, mode } => {
                let thread_id = self.thread(thread)?;
                let mode = match mode.as_deref().unwrap_or("auto") {
                    "auto" => DispatchMode::Auto,
                    "queue" => DispatchMode::QueueAfterActive,
                    "steer" => DispatchMode::SteerActive {
                        target_run_id: self.active_run(thread_id)?,
                    },
                    "restart" => DispatchMode::RestartActive {
                        target_run_id: self.active_run(thread_id)?,
                    },
                    other => return Err(ReplayError::Harness(format!("unknown mode {other}"))),
                };
                let message_id = MessageId(self.next_id(0xd));
                self.dispatch(Command::MessageDispatch {
                    thread_id,
                    message_id,
                    text: text.clone(),
                    display_text: None,
                    attachments: Vec::new(),
                    context: Vec::new(),
                    model_selection: None,
                    mode,
                    title_seed: None,
                    source_plan: None,
                })?;
            }
            ScenarioStep::Steer { text, thread } => {
                return self.step(
                    &ScenarioStep::Message {
                        text: text.clone(),
                        thread: thread.clone(),
                        mode: Some("steer".to_owned()),
                    },
                    forks,
                );
            }
            ScenarioStep::Wait { until, thread } => {
                let thread_id = self.thread(thread)?;
                return self.wait_until(thread_id, until);
            }
            ScenarioStep::Interrupt { thread } => {
                let thread_id = self.thread(thread)?;
                let run_id = self.active_run(thread_id)?;
                self.dispatch(Command::RunInterrupt {
                    thread_id,
                    run_id,
                    reason: None,
                    hold_queue: false,
                })?;
            }
            ScenarioStep::Respond {
                request,
                option,
                thread,
            } => {
                let thread_id = self.thread(thread)?;
                let request_id = self.pending_request(thread_id, request)?;
                self.dispatch(Command::RuntimeRequestRespond {
                    thread_id,
                    request_id,
                    response: RuntimeResponse::Approval {
                        option_id: option.clone(),
                    },
                })?;
            }
            ScenarioStep::Answer {
                request,
                answers,
                thread,
            } => {
                let thread_id = self.thread(thread)?;
                let request_id = self.pending_request(thread_id, request)?;
                self.dispatch(Command::RuntimeRequestRespond {
                    thread_id,
                    request_id,
                    response: RuntimeResponse::UserInput {
                        answers: answers.clone(),
                    },
                })?;
            }
            ScenarioStep::Rollback {
                runs,
                restore_files,
                thread,
            } => {
                let thread_id = self.thread(thread)?;
                let projection = self.projection(thread_id)?;
                let latest = projection.latest_ordinal();
                let target = latest.saturating_sub(*runs);
                let checkpoint = projection
                    .checkpoints
                    .values()
                    .filter(|checkpoint| checkpoint.app_run_ordinal == Some(target))
                    .max_by_key(|checkpoint| checkpoint.ordinal_within_scope)
                    .cloned()
                    .ok_or_else(|| {
                        ReplayError::Harness(format!("no checkpoint after run {target}"))
                    })?;
                self.dispatch(Command::CheckpointRollback {
                    thread_id,
                    scope_id: checkpoint.scope_id,
                    checkpoint_id: checkpoint.id,
                    restore_files: *restore_files,
                })?;
            }
            ScenarioStep::Fork {
                at_run,
                name,
                thread,
            } => {
                let source = self.thread(thread)?;
                let after_run = match at_run {
                    Some(ordinal) => Some(
                        self.projection(source)?
                            .runs
                            .values()
                            .find(|run| run.ordinal == Some(*ordinal))
                            .map(|run| run.id)
                            .ok_or_else(|| ReplayError::Harness(format!("no run {ordinal}")))?,
                    ),
                    None => None,
                };
                let target = self.create_thread(name)?;
                let manager = self.manager.clone();
                let result = self
                    .runtime
                    .block_on(manager.fork_thread(source, after_run, target))
                    .map_err(harness_error)?;
                forks.push(result);
            }
            ScenarioStep::Select { selection, thread } => {
                let thread_id = self.thread(thread)?;
                let mut model_selection = ModelSelection::new(self.instance.instance_id.as_str());
                model_selection.model = selection.model.clone();
                model_selection.reasoning_effort = selection.reasoning_effort.clone();
                self.dispatch(Command::ThreadSetModelSelection {
                    thread_id,
                    model_selection,
                })?;
            }
            ScenarioStep::Compact { thread } => {
                let thread_id = self.thread(thread)?;
                let manager = self.manager.clone();
                self.runtime
                    .block_on(manager.compact(thread_id))
                    .map_err(harness_error)?;
            }
            ScenarioStep::ReleaseIdle => {
                let manager = self.manager.clone();
                let released = self.runtime.block_on(manager.release_idle());
                self.released.extend(released);
            }
            ScenarioStep::Unload { thread } => {
                let thread_id = self.thread(thread)?;
                let manager = self.manager.clone();
                self.runtime
                    .block_on(manager.unload_thread(thread_id))
                    .map_err(harness_error)?;
            }
        }
        self.settle()
    }

    fn pending_request(
        &self,
        thread_id: ThreadId,
        which: &str,
    ) -> Result<RuntimeRequestId, ReplayError> {
        if which != "first_pending" {
            return Err(ReplayError::Harness(format!(
                "unknown request selector {which}"
            )));
        }
        self.projection(thread_id)?
            .runtime_requests
            .values()
            .filter(|request| request.status == RequestStatus::Pending)
            .min_by_key(|request| (request.created_at, request.id))
            .map(|request| request.id)
            .ok_or_else(|| ReplayError::Harness("no pending request".to_owned()))
    }

    /// Runs every step, closes the sessions, and checks the transcript was
    /// fully consumed and the projections rebuild from the log.
    pub fn run(&mut self, input: &ScenarioInput) -> Result<ScenarioResult, ReplayError> {
        let mut forks = Vec::new();
        self.settle()?;
        if let Some(runtime_mode) = input.runtime_mode {
            let thread_id = self.thread(&None)?;
            self.dispatch(Command::ThreadSetRuntimeMode {
                thread_id,
                runtime_mode,
            })?;
        }
        for step in &input.steps {
            self.step(step, &mut forks)?;
        }
        let manager = self.manager.clone();
        self.runtime.block_on(manager.shutdown());
        self.settle()?;
        if let Some(replay) = &self.replay {
            replay.assert_exhausted()?;
        }
        let report = self.orchestrator.verify().map_err(harness_error)?;
        if !report.is_clean() {
            return Err(ReplayError::Harness(format!(
                "rebuilding the projections from the log differs from the live state: {report:#?}"
            )));
        }
        let main = self.thread(&None)?;
        let mut threads = BTreeMap::new();
        for (name, id) in &self.threads {
            threads.insert(name.clone(), (*self.projection(*id)?).clone());
        }
        let events = self.orchestrator.reader().with(|connection| {
            let mut statement = connection
                .prepare("SELECT event_type, payload_json FROM events ORDER BY sequence")
                .map_err(harness_error)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(harness_error)?
                .filter_map(Result::ok)
                .filter_map(|(kind, payload)| DomainEvent::from_parts(&kind, &payload).ok())
                .collect::<Vec<_>>();
            Ok::<_, ReplayError>(rows)
        })?;
        let projection = self.projection(main)?;
        let mut checkpoints = projection.checkpoints.values().cloned().collect::<Vec<_>>();
        checkpoints.sort_by_key(|checkpoint| {
            (checkpoint.app_run_ordinal, checkpoint.ordinal_within_scope)
        });
        let capabilities = self
            .manager
            .registry()
            .get(&self.instance.instance_id)
            .map(|entry| entry.driver.capabilities(&entry.config))
            .unwrap_or_default();
        Ok(ScenarioResult {
            projection: (*projection).clone(),
            threads,
            events,
            adapter_events: self.adapter_events.lock().clone(),
            checkpoints,
            capabilities,
            forks,
            checkpoint_calls: self.ports.checkpoints.calls.lock().clone(),
            released: self.released.clone(),
            workspace: self.workspace.clone(),
        })
    }
}

impl Drop for ReplayHarness {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            pool.shutdown(Duration::from_secs(1));
        }
        self.orchestrator.shutdown();
    }
}

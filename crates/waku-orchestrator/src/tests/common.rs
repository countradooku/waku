//! Test environment: a temporary `state.sqlite`, a manual clock, sequential
//! ids, and a stand-in driver that ingests normalized adapter events.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tempfile::TempDir;
use uuid::Uuid;
use waku_protocol::orchestration::*;

use crate::clock::ManualClock;
use crate::decide::Decision;
use crate::fakes::StaticCapabilities;
use crate::ids::{SequentialIds, derived_id};
use crate::orchestrator::{
    Dependencies, DispatchError, DispatchReceipt, IngestOutcome, Orchestrator, OrchestratorConfig,
};
use crate::projection::ThreadProjection;
use crate::provider::{
    AdapterEvent, IngestEnvelope, RunRef, ThreadDisposition, TurnOutcome, TurnTerminal,
};
use crate::store::outbox::{self, EffectStatus};
use crate::store::sink::WriteGuard;

pub const INSTANCE: &str = "codex";
pub const START: Timestamp = 1_700_000_000_000;

static NEXT: AtomicU64 = AtomicU64::new(1);

/// A process-unique id for test entities.
pub fn fresh<T: From<Uuid>>() -> T {
    T::from(Uuid::from_u128(
        (0xfeed_u128 << 96) | NEXT.fetch_add(1, Ordering::Relaxed) as u128,
    ))
}

pub fn cid(id: &str) -> CommandId {
    CommandId::new(id).unwrap()
}

pub fn run_for(message: MessageId) -> RunId {
    RunId(derived_id("run", &message.to_string()))
}

pub fn steering_capabilities() -> Capabilities {
    let mut capabilities = Capabilities::default();
    capabilities.turns.active_steering = true;
    capabilities.turns.interrupt = true;
    capabilities
}

pub struct Env {
    pub dir: TempDir,
    pub clock: Arc<ManualClock>,
    pub capabilities: Arc<StaticCapabilities>,
    pub orchestrator: Arc<Orchestrator>,
    commands: AtomicU64,
}

impl Env {
    pub fn new() -> Self {
        Self::with_capabilities(Capabilities::default())
    }

    pub fn with_capabilities(capabilities: Capabilities) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(ManualClock::new(START));
        let capabilities = Arc::new(StaticCapabilities::with(INSTANCE, capabilities));
        let orchestrator = open(&dir, &clock, &capabilities, 0);
        Self {
            dir,
            clock,
            capabilities,
            orchestrator,
            commands: AtomicU64::new(0),
        }
    }

    /// Drops the orchestrator and opens the same file again, like a restart.
    pub fn reopen(&mut self) {
        let generation = self.commands.fetch_add(1_000_000, Ordering::Relaxed) + 1;
        self.orchestrator = open(&self.dir, &self.clock, &self.capabilities, generation);
    }

    pub fn tick(&self) -> Timestamp {
        self.clock.advance(1)
    }

    pub fn dispatch(&self, id: &str, command: Command) -> Result<DispatchReceipt, DispatchError> {
        self.tick();
        self.orchestrator.dispatch(CommandEnvelope {
            command_id: cid(id),
            issued_by: Actor::User,
            source: CreationSource::Desktop,
            command,
        })
    }

    /// Dispatches with a fresh command id and expects success.
    pub fn ok(&self, command: Command) -> DispatchReceipt {
        let n = self.commands.fetch_add(1, Ordering::Relaxed);
        let kind = command.command_type();
        self.dispatch(&format!("c{n}"), command)
            .unwrap_or_else(|error| panic!("{kind}: {error}"))
    }

    pub fn try_ok(&self, command: Command) -> Result<DispatchReceipt, DispatchError> {
        let n = self.commands.fetch_add(1, Ordering::Relaxed);
        self.dispatch(&format!("c{n}"), command)
    }

    pub fn project(&self) -> ProjectId {
        let project_id = fresh();
        self.ok(Command::ProjectCreate {
            project_id,
            name: "Project".into(),
            path: PathBuf::from("/repo"),
            order_key: None,
        });
        project_id
    }

    pub fn thread(&self, project_id: ProjectId) -> ThreadId {
        let thread_id = fresh();
        self.ok(Command::ThreadCreate {
            thread_id,
            project_id,
            title: None,
            model_selection: ModelSelection::new(INSTANCE),
            runtime_mode: RuntimeMode::FullAccess,
            interaction_mode: InteractionMode::Default,
            workspace: WorkspaceStrategy::Root { branch: None },
            imported_native_thread: None,
        });
        thread_id
    }

    pub fn send_command(
        thread_id: ThreadId,
        message_id: MessageId,
        text: &str,
        mode: DispatchMode,
    ) -> Command {
        Command::MessageDispatch {
            thread_id,
            message_id,
            text: text.into(),
            display_text: None,
            attachments: Vec::new(),
            context: Vec::new(),
            model_selection: None,
            mode,
            title_seed: None,
            source_plan: None,
        }
    }

    /// Sends a message and returns the run it created.
    pub fn send(&self, thread_id: ThreadId, text: &str, mode: DispatchMode) -> RunId {
        let message_id = fresh();
        self.ok(Self::send_command(thread_id, message_id, text, mode));
        run_for(message_id)
    }

    pub fn projection(&self, thread_id: ThreadId) -> Arc<ThreadProjection> {
        self.orchestrator.thread(thread_id).unwrap()
    }

    pub fn run(&self, thread_id: ThreadId, run_id: RunId) -> Run {
        self.projection(thread_id).runs[&run_id].clone()
    }

    pub fn count(&self, table: &str) -> i64 {
        self.orchestrator.reader().with(|connection| {
            connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap()
        })
    }

    pub fn outbox(&self) -> Vec<(EffectId, String, EffectStatus)> {
        outbox::list(&self.orchestrator.store().writer()).unwrap()
    }

    pub fn ingest(
        &self,
        thread_id: ThreadId,
        decision: Decision,
        guard: WriteGuard,
    ) -> IngestOutcome {
        self.tick();
        self.orchestrator
            .ingest_decision(thread_id, decision, guard)
            .unwrap()
    }

    pub fn assert_verified(&self) {
        let report = self.orchestrator.verify().unwrap();
        assert!(report.is_clean(), "{report:#?}");
    }
}

fn open(
    dir: &TempDir,
    clock: &Arc<ManualClock>,
    capabilities: &Arc<StaticCapabilities>,
    generation: u64,
) -> Arc<Orchestrator> {
    let mut config = OrchestratorConfig::new(dir.path().join("state.sqlite"));
    config.read_connections = 2;
    config.cache_threads = 8;
    Orchestrator::open(
        config,
        Dependencies {
            clock: clock.clone(),
            ids: Box::new(SequentialIds::new(0x1000 + generation)),
            capabilities: capabilities.clone(),
        },
    )
    .unwrap()
}

// -- provider ingestion ------------------------------------------------------------
//
// A driver's normalized events, ingested through `Orchestrator::ingest` and
// `decide_ingest` exactly as the session manager does (RFC 0002).

fn derived<T: From<Uuid>>(scope: &str, key: impl std::fmt::Display) -> T {
    T::from(derived_id(scope, &key.to_string()))
}

/// The run's current attempt and root node.
fn current(
    projection: &ThreadProjection,
    run_id: RunId,
) -> Option<(Run, RunAttempt, ExecutionNode)> {
    let run = projection.runs.get(&run_id)?.clone();
    let attempt = projection.attempts.get(&run.active_attempt_id?)?.clone();
    let node = projection.nodes.get(&run.root_node_id?)?.clone();
    Some((run, attempt, node))
}

fn run_ref(run: &Run, attempt: &RunAttempt, node: &ExecutionNode) -> RunRef {
    RunRef {
        thread_id: run.thread_id,
        run_id: run.id,
        attempt_id: attempt.id,
        root_node_id: node.id,
    }
}

/// The provider session a stand-in driver uses for a provider thread.
pub fn session_for(provider_thread: ProviderThreadId) -> ProviderSessionId {
    derived("session", provider_thread)
}

impl Env {
    /// Ingests one adapter event and expects the storage to accept it.
    pub fn adapter(&self, envelope: IngestEnvelope) -> IngestOutcome {
        self.tick();
        self.orchestrator.ingest(envelope).unwrap()
    }
}

/// The provider accepted the turn: session, native thread, turn, running.
pub fn go_running(env: &Env, thread_id: ThreadId, run_id: RunId) -> Option<IngestOutcome> {
    let projection = env.projection(thread_id);
    let (run, attempt, node) = current(&projection, run_id)?;
    if run.status != RunStatus::Starting {
        return None;
    }
    let now = env.clock.advance(1);
    let provider_thread = projection
        .provider_threads
        .get(&attempt.provider_thread_id)?
        .clone();
    let session_id = session_for(provider_thread.id);
    let capabilities = env
        .capabilities
        .0
        .lock()
        .get(&provider_thread.provider_instance_id)
        .cloned()
        .unwrap_or_default();
    let reference = run_ref(&run, &attempt, &node);
    env.adapter(IngestEnvelope::for_thread(
        thread_id,
        session_id,
        1,
        AdapterEvent::SessionUpdated {
            session: ProviderSession {
                id: session_id,
                driver: provider_thread.driver,
                provider_instance_id: provider_thread.provider_instance_id.clone(),
                status: SessionStatus::Running,
                cwd: PathBuf::from("/repo"),
                model: None,
                capabilities,
                created_at: now,
                updated_at: now,
                last_error: None,
                detached_at: None,
                detach_reason: None,
            },
        },
    ));
    env.adapter(IngestEnvelope::for_thread(
        thread_id,
        session_id,
        1,
        AdapterEvent::ProviderThreadUpdated {
            thread: ProviderThread {
                provider_session_id: Some(session_id),
                native_thread_ref: Some(ProviderRef::strong(
                    provider_thread.driver,
                    format!("native-{}", provider_thread.id),
                )),
                updated_at: now,
                ..provider_thread.clone()
            },
            retained_native_turn_ids: None,
        },
    ));
    let projection = env.projection(thread_id);
    let turn = ProviderTurn {
        id: derived("turn", attempt.id),
        provider_thread_id: provider_thread.id,
        node_id: node.id,
        run_attempt_id: Some(attempt.id),
        native_turn_ref: Some(ProviderRef::strong(
            provider_thread.driver,
            format!("turn-{}", attempt.id),
        )),
        ordinal: projection
            .provider_turns
            .values()
            .filter(|turn| turn.provider_thread_id == provider_thread.id)
            .count() as u32
            + 1,
        status: ProviderTurnStatus::Running,
        started_at: Some(now),
        completed_at: None,
        token_usage: None,
        turn_token_usage: None,
    };
    Some(env.adapter(IngestEnvelope::for_run(
        session_id,
        1,
        &reference,
        AdapterEvent::ProviderTurnUpdated { turn },
    )))
}

fn live_session(projection: &ThreadProjection, attempt: &RunAttempt) -> ProviderSessionId {
    projection
        .provider_threads
        .get(&attempt.provider_thread_id)
        .and_then(|provider_thread| provider_thread.provider_session_id)
        .unwrap_or_else(|| session_for(attempt.provider_thread_id))
}

/// A streamed assistant text snapshot (R12), upserting one item per run.
pub fn assistant_text(
    env: &Env,
    thread_id: ThreadId,
    run_id: RunId,
    text: &str,
) -> Option<IngestOutcome> {
    let projection = env.projection(thread_id);
    let (run, attempt, node) = current(&projection, run_id)?;
    if !matches!(run.status, RunStatus::Running | RunStatus::Waiting) {
        return None;
    }
    let now = env.clock.advance(1);
    let session = live_session(&projection, &attempt);
    let reference = run_ref(&run, &attempt, &node);
    let message_id: MessageId = derived("assistant", attempt.id);
    let item_id: TurnItemId = derived("assistant_item", attempt.id);
    env.adapter(IngestEnvelope::for_run(
        session,
        1,
        &reference,
        AdapterEvent::MessageUpdated {
            message: Message {
                id: message_id,
                thread_id,
                run_id: Some(run.id),
                node_id: Some(node.id),
                role: MessageRole::Assistant,
                created_by: Actor::Agent,
                creation_source: CreationSource::Provider,
                text: text.to_owned(),
                display_text: None,
                attachments: Vec::new(),
                context: Vec::new(),
                notification: None,
                sender_thread_id: None,
                streaming: true,
                created_at: now,
                updated_at: now,
            },
        },
    ));
    Some(env.adapter(IngestEnvelope::for_run(
        session,
        1,
        &reference,
        AdapterEvent::TurnItemUpdated {
            item: TurnItem {
                id: item_id,
                thread_id,
                run_id: Some(run.id),
                node_id: Some(node.id),
                provider_thread_id: Some(attempt.provider_thread_id),
                provider_turn_id: attempt.provider_turn_id,
                native_item_ref: None,
                parent_item_id: None,
                position: 0,
                status: ItemStatus::Running,
                title: None,
                tool: None,
                stream_rev: text.len() as u32,
                started_at: Some(now),
                completed_at: None,
                updated_at: now,
                kind: TurnItemKind::AssistantMessage {
                    message_id,
                    text: text.to_owned(),
                    streaming: true,
                },
            },
        },
    )))
}

/// The provider asks for an approval: the run waits on the user.
pub fn request_approval(
    env: &Env,
    thread_id: ThreadId,
    run_id: RunId,
) -> Option<(IngestOutcome, RuntimeRequestId)> {
    let projection = env.projection(thread_id);
    let (run, attempt, node) = current(&projection, run_id)?;
    if run.status != RunStatus::Running {
        return None;
    }
    let session = projection
        .provider_threads
        .get(&attempt.provider_thread_id)?
        .provider_session_id?;
    let now = env.clock.advance(1);
    let request_id: RuntimeRequestId = fresh();
    let outcome = env.adapter(IngestEnvelope::for_run(
        session,
        1,
        &run_ref(&run, &attempt, &node),
        AdapterEvent::RuntimeRequestUpdated {
            request: RuntimeRequest {
                id: request_id,
                thread_id,
                node_id: node.id,
                provider_turn_id: attempt.provider_turn_id,
                native_request_ref: None,
                kind: RuntimeRequestKind::CommandApproval,
                status: RequestStatus::Pending,
                response_capability: ResponseCapability::Live {
                    provider_session_id: session,
                },
                options: vec![PermissionOption {
                    id: "allow_once".into(),
                    label: "Allow".into(),
                    allow: true,
                }],
                questions: Vec::new(),
                response: None,
                created_at: now,
                resolved_at: None,
            },
        },
    ));
    Some((outcome, request_id))
}

/// The driver confirms answered requests: resolved, the run runs again.
pub fn resolve_requests(env: &Env, thread_id: ThreadId, run_id: RunId) -> Option<IngestOutcome> {
    let projection = env.projection(thread_id);
    let (run, attempt, node) = current(&projection, run_id)?;
    if run.status != RunStatus::Waiting {
        return None;
    }
    let now = env.clock.advance(1);
    let session = live_session(&projection, &attempt);
    let reference = run_ref(&run, &attempt, &node);
    let mut outcome = None;
    for request in projection
        .runtime_requests
        .values()
        .filter(|request| request.status == RequestStatus::Pending)
    {
        outcome = Some(env.adapter(IngestEnvelope::for_run(
            session,
            1,
            &reference,
            AdapterEvent::RuntimeRequestUpdated {
                request: RuntimeRequest {
                    status: RequestStatus::Resolved,
                    resolved_at: Some(now),
                    ..request.clone()
                },
            },
        )));
    }
    outcome
}

/// The provider's terminal event: the run enters the finalization barrier,
/// which captures a checkpoint.
pub fn provider_done(env: &Env, thread_id: ThreadId, run_id: RunId) -> Option<IngestOutcome> {
    let projection = env.projection(thread_id);
    let (run, attempt, node) = current(&projection, run_id)?;
    if run.status != RunStatus::Running {
        return None;
    }
    let session = live_session(&projection, &attempt);
    let reference = run_ref(&run, &attempt, &node);
    Some(env.adapter(IngestEnvelope::for_run(
        session,
        1,
        &reference,
        AdapterEvent::TurnTerminal {
            terminal: TurnTerminal {
                run: reference,
                provider_turn: None,
                outcome: TurnOutcome::Completed,
                thread_disposition: ThreadDisposition::Reusable,
                child_turns_continue: false,
                background_pending: false,
                summary: None,
            },
        },
    )))
}

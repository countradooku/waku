//! Test environment: a temporary `state.sqlite`, a manual clock, sequential
//! ids, and stand-ins for provider ingestion (#44 brings the real one).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tempfile::TempDir;
use uuid::Uuid;
use waku_protocol::orchestration::*;

use crate::clock::ManualClock;
use crate::command::{CapturedCheckpoint, InternalCommand};
use crate::decide::Decision;
use crate::effects::Effect;
use crate::fakes::StaticCapabilities;
use crate::ids::{SequentialIds, derived_id};
use crate::orchestrator::{
    Dependencies, DispatchError, DispatchReceipt, IngestOutcome, Orchestrator, OrchestratorConfig,
};
use crate::projection::ThreadProjection;
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

// -- provider ingestion stand-ins ------------------------------------------------

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

fn guard_for(attempt: &RunAttempt) -> WriteGuard {
    WriteGuard::RunCurrent {
        run_id: attempt.run_id,
        attempt_id: attempt.id,
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
    let session_id: ProviderSessionId = derived("session", provider_thread.id);
    let capabilities = env
        .capabilities
        .0
        .lock()
        .get(&provider_thread.provider_instance_id)
        .cloned()
        .unwrap_or_default();
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
    let decision = Decision {
        events: vec![
            DomainEvent::ProviderSessionUpdated(ProviderSession {
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
            }),
            DomainEvent::ProviderThreadUpdated(ProviderThread {
                provider_session_id: Some(session_id),
                native_thread_ref: Some(ProviderRef::strong(
                    provider_thread.driver,
                    format!("native-{}", provider_thread.id),
                )),
                status: ProviderThreadStatus::Active,
                updated_at: now,
                ..provider_thread
            }),
            DomainEvent::ProviderTurnUpdated(turn.clone()),
            DomainEvent::RunAttemptUpdated(RunAttempt {
                provider_turn_id: Some(turn.id),
                status: AttemptStatus::Running,
                started_at: Some(now),
                ..attempt.clone()
            }),
            DomainEvent::NodeUpdated(ExecutionNode {
                status: NodeStatus::Running,
                provider_turn_id: Some(turn.id),
                started_at: Some(now),
                ..node
            }),
            DomainEvent::RunUpdated(Run {
                status: RunStatus::Running,
                started_at: Some(now),
                ..run
            }),
        ],
        ..Decision::default()
    };
    Some(env.ingest(thread_id, decision, guard_for(&attempt)))
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
    let message_id: MessageId = derived("assistant", attempt.id);
    let item_id: TurnItemId = derived("assistant_item", attempt.id);
    let block = run.ordinal?;
    let position = projection
        .hot_items
        .values()
        .find(|item| item.id == item_id)
        .map(|item| item.position)
        .unwrap_or_else(|| {
            TurnItem::position_of(
                block,
                projection.next_position.get(&block).copied().unwrap_or(0),
            )
        });
    let decision = Decision {
        events: vec![
            DomainEvent::MessageUpdated(Message {
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
            }),
            DomainEvent::TurnItemUpdated(TurnItem {
                id: item_id,
                thread_id,
                run_id: Some(run.id),
                node_id: Some(node.id),
                provider_thread_id: Some(attempt.provider_thread_id),
                provider_turn_id: attempt.provider_turn_id,
                native_item_ref: None,
                parent_item_id: None,
                position,
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
            }),
        ],
        ..Decision::default()
    };
    Some(env.ingest(thread_id, decision, guard_for(&attempt)))
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
    let request_node: NodeId = derived("request_node", request_id);
    let block = run.ordinal?;
    let position = TurnItem::position_of(
        block,
        projection.next_position.get(&block).copied().unwrap_or(0),
    );
    let options = vec![PermissionOption {
        id: "allow_once".into(),
        label: "Allow".into(),
        allow: true,
    }];
    let decision = Decision {
        events: vec![
            DomainEvent::NodeUpdated(ExecutionNode {
                id: request_node,
                parent_node_id: Some(node.id),
                kind: NodeKind::ApprovalRequest,
                status: NodeStatus::Waiting,
                counts_for_run: false,
                runtime_request_id: Some(request_id),
                checkpoint_scope_id: None,
                started_at: Some(now),
                completed_at: None,
                ..node.clone()
            }),
            DomainEvent::RuntimeRequestUpdated(RuntimeRequest {
                id: request_id,
                thread_id,
                node_id: request_node,
                provider_turn_id: attempt.provider_turn_id,
                native_request_ref: None,
                kind: RuntimeRequestKind::CommandApproval,
                status: RequestStatus::Pending,
                response_capability: ResponseCapability::Live {
                    provider_session_id: session,
                },
                options: options.clone(),
                questions: Vec::new(),
                response: None,
                created_at: now,
                resolved_at: None,
            }),
            DomainEvent::TurnItemUpdated(TurnItem {
                id: derived("request_item", request_id),
                thread_id,
                run_id: Some(run.id),
                node_id: Some(request_node),
                provider_thread_id: Some(attempt.provider_thread_id),
                provider_turn_id: attempt.provider_turn_id,
                native_item_ref: None,
                parent_item_id: None,
                position,
                status: ItemStatus::Waiting,
                title: None,
                tool: None,
                stream_rev: 0,
                started_at: Some(now),
                completed_at: None,
                updated_at: now,
                kind: TurnItemKind::ApprovalRequest {
                    request_id,
                    request_kind: RuntimeRequestKind::CommandApproval,
                    prompt: Some("run ls?".into()),
                    app_name: None,
                    options,
                },
            }),
            DomainEvent::RunUpdated(Run {
                status: RunStatus::Waiting,
                ..run
            }),
        ],
        ..Decision::default()
    };
    Some((
        env.ingest(thread_id, decision, guard_for(&attempt)),
        request_id,
    ))
}

/// The driver confirms an answered request: resolved, the run runs again.
pub fn resolve_requests(env: &Env, thread_id: ThreadId, run_id: RunId) -> Option<IngestOutcome> {
    let projection = env.projection(thread_id);
    let (run, attempt, _) = current(&projection, run_id)?;
    if run.status != RunStatus::Waiting {
        return None;
    }
    let now = env.clock.advance(1);
    let mut events = projection
        .runtime_requests
        .values()
        .filter(|request| request.status == RequestStatus::Pending)
        .map(|request| {
            DomainEvent::RuntimeRequestUpdated(RuntimeRequest {
                status: RequestStatus::Resolved,
                resolved_at: Some(now),
                ..request.clone()
            })
        })
        .collect::<Vec<_>>();
    events.push(DomainEvent::RunUpdated(Run {
        status: RunStatus::Running,
        ..run
    }));
    Some(env.ingest(
        thread_id,
        Decision {
            events,
            ..Decision::default()
        },
        guard_for(&attempt),
    ))
}

/// The provider's terminal event: the run enters the finalization barrier,
/// which captures a checkpoint.
pub fn provider_done(env: &Env, thread_id: ThreadId, run_id: RunId) -> Option<IngestOutcome> {
    let projection = env.projection(thread_id);
    let (run, attempt, node) = current(&projection, run_id)?;
    if run.status != RunStatus::Running {
        return None;
    }
    let now = env.clock.advance(1);
    let mut events = Vec::new();
    if let Some(turn) = attempt
        .provider_turn_id
        .and_then(|id| projection.provider_turns.get(&id))
    {
        events.push(DomainEvent::ProviderTurnUpdated(ProviderTurn {
            status: ProviderTurnStatus::Completed,
            completed_at: Some(now),
            ..turn.clone()
        }));
    }
    events.push(DomainEvent::RunAttemptUpdated(RunAttempt {
        status: AttemptStatus::Completed,
        completed_at: Some(now),
        ..attempt.clone()
    }));
    events.push(DomainEvent::RunUpdated(Run {
        status: RunStatus::Finalizing,
        provider_completed_at: Some(now),
        ..run.clone()
    }));
    let effects = node
        .checkpoint_scope_id
        .map(|scope_id| {
            vec![Effect::CheckpointCapture {
                run_id: Some(run.id),
                scope_id,
            }]
        })
        .unwrap_or_default();
    Some(env.ingest(
        thread_id,
        Decision {
            events,
            effects,
            ..Decision::default()
        },
        guard_for(&attempt),
    ))
}

/// A finished assistant message for the run's current attempt.
pub fn final_assistant(env: &Env, thread_id: ThreadId, run_id: RunId, text: &str) {
    let projection = env.projection(thread_id);
    let run = projection.runs[&run_id].clone();
    let attempt = projection.attempts[&run.active_attempt_id.unwrap()].clone();
    let node = run.root_node_id.unwrap();
    let block = run.ordinal.unwrap();
    let now = env.clock.advance(1);
    let message_id = MessageId(derived_id("assistant", &attempt.id.to_string()));
    let position = TurnItem::position_of(
        block,
        projection.next_position.get(&block).copied().unwrap_or(0),
    );
    let decision = Decision {
        events: vec![
            DomainEvent::MessageUpdated(Message {
                id: message_id,
                thread_id,
                run_id: Some(run_id),
                node_id: Some(node),
                role: MessageRole::Assistant,
                created_by: Actor::Agent,
                creation_source: CreationSource::Provider,
                text: text.to_owned(),
                display_text: None,
                attachments: Vec::new(),
                context: Vec::new(),
                notification: None,
                sender_thread_id: None,
                streaming: false,
                created_at: now,
                updated_at: now,
            }),
            DomainEvent::TurnItemUpdated(TurnItem {
                id: TurnItemId(derived_id("assistant_item", &attempt.id.to_string())),
                thread_id,
                run_id: Some(run_id),
                node_id: Some(node),
                provider_thread_id: Some(attempt.provider_thread_id),
                provider_turn_id: attempt.provider_turn_id,
                native_item_ref: None,
                parent_item_id: None,
                position,
                status: ItemStatus::Completed,
                title: None,
                tool: None,
                stream_rev: 0,
                started_at: Some(now),
                completed_at: Some(now),
                updated_at: now,
                kind: TurnItemKind::AssistantMessage {
                    message_id,
                    text: text.to_owned(),
                    streaming: false,
                },
            }),
        ],
        ..Decision::default()
    };
    env.ingest(
        thread_id,
        decision,
        WriteGuard::RunCurrent {
            run_id,
            attempt_id: attempt.id,
        },
    );
}

/// A whole turn: the user's message, the reply, the finalization barrier.
pub fn turn(env: &Env, thread_id: ThreadId, user: &str, reply: &str) -> RunId {
    let run_id = env.send(thread_id, user, DispatchMode::Auto);
    go_running(env, thread_id, run_id).unwrap();
    final_assistant(env, thread_id, run_id, reply);
    provider_done(env, thread_id, run_id).unwrap();
    let scope_id = {
        let projection = env.projection(thread_id);
        let node = projection.runs[&run_id].root_node_id.unwrap();
        projection.nodes[&node].checkpoint_scope_id.unwrap()
    };
    env.tick();
    env.orchestrator
        .dispatch_internal(
            cid(&format!("capture:{run_id}")),
            InternalCommand::CheckpointCaptured {
                thread_id,
                run_id: Some(run_id),
                scope_id,
                result: Ok(CapturedCheckpoint {
                    git_ref: format!("refs/waku/t/{thread_id}/{scope_id}/0"),
                    files: Vec::new(),
                }),
            },
        )
        .unwrap();
    assert_eq!(env.run(thread_id, run_id).status, RunStatus::Completed);
    run_id
}

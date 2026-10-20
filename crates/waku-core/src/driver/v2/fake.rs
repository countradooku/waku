//! The fake driver: a complete `DriverV2` over a small JSON-lines protocol
//! (`fake.jsonl`), used to prove the trait, the session manager and the
//! replay harness before any real driver is ported (#44, #45).
//!
//! It is written the way a port is (RFC 0002 "Port checklist"): one actor per
//! session behind `&self` methods, processes only through
//! `ctx.transports`, native ids bound through `ctx.ids`, text routed through
//! the `DeltaCoalescer`, tool output through `CommandOutputDelta`, unknown
//! frames to the raw log, and exactly one `TurnTerminal` per accepted turn on
//! every path (completion, failure, interrupt before registration, process
//! exit). [`super::fake_agent`] is the "CLI" on the other end.
//!
//! Protocol (JSON-RPC-shaped, one object per line):
//!
//! ```text
//! → {"id":n,"method":"initialize"|"thread/start"|"thread/resume"|"turn/start"|"turn/steer"
//!            |"turn/interrupt"|"thread/rollback"|"thread/fork"|"thread/compact"|"subagent/stop",
//!    "params":{…}}
//! ← {"id":n,"result":{…}} | {"id":n,"error":{"code":"…","message":"…"}}
//! ← {"method":"turn/started"|"item/delta"|"item/completed"|"tool/started"|"tool/output"
//!            |"tool/completed"|"plan/updated"|"subagent/updated"|"subagent/turn_completed"
//!            |"turn/completed","params":{…}}
//! ← {"id":"r1","method":"approval/request"|"question/request","params":{…}}   (agent → driver)
//! → {"id":"r1","result":{"optionId":"…"} | {"answers":{…}} | {"dismissed":true}}
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::oneshot;
use waku_protocol::orchestration::{self as orch, *};

use super::RuntimeResponse;
use super::coalescer::DeltaCoalescer;
use super::*;

pub const FAKE_PROVIDER: &str = "fake";
pub const FAKE_PROTOCOL: &str = "fake.jsonl";

/// How a fake instance behaves, from `InstanceConfig::extra`:
/// `{"steering": "native"|"restart", "fork": bool, "rollback": bool,
/// "background": bool}`. `background` reports pending background work, which
/// pins an idle session for up to 4 hours.|"restart", "fork": bool, "rollback": bool}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FakeProfile {
    pub native_steering: bool,
    pub fork: bool,
    pub rollback: bool,
    pub background_work: bool,
}

impl Default for FakeProfile {
    fn default() -> Self {
        Self {
            native_steering: true,
            fork: true,
            rollback: true,
            background_work: false,
        }
    }
}

impl FakeProfile {
    pub fn from_config(cfg: &InstanceConfig) -> Self {
        let default = Self::default();
        let extra = &cfg.extra;
        Self {
            native_steering: extra
                .get("steering")
                .and_then(Value::as_str)
                .map_or(default.native_steering, |mode| mode != "restart"),
            fork: extra
                .get("fork")
                .and_then(Value::as_bool)
                .unwrap_or(default.fork),
            rollback: extra
                .get("rollback")
                .and_then(Value::as_bool)
                .unwrap_or(default.rollback),
            background_work: extra
                .get("background")
                .and_then(Value::as_bool)
                .unwrap_or(default.background_work),
        }
    }

    pub fn to_extra(self) -> Value {
        json!({
            "steering": if self.native_steering { "native" } else { "restart" },
            "fork": self.fork,
            "rollback": self.rollback,
            "background": self.background_work,
        })
    }
}

/// An instance of the fake provider.
pub fn fake_instance(profile: FakeProfile) -> InstanceConfig {
    let mut cfg = InstanceConfig::new(FAKE_PROVIDER, orch::DriverKind::Unknown);
    cfg.binary = "fake-agent".into();
    cfg.cli_version = Some("1.0.0".to_owned());
    cfg.extra = profile.to_extra();
    cfg
}

/// The fake's declared capabilities: what this driver really does.
pub fn fake_capabilities(profile: FakeProfile) -> Capabilities {
    let mut caps = Capabilities::default();
    caps.sessions.model_switch_in_session = true;
    caps.sessions.provider_switching_via_handoff = true;
    caps.threads.create_empty_thread = true;
    caps.threads.native_thread_id = true;
    caps.threads.rollback = profile.rollback;
    caps.threads.fork = profile.fork;
    caps.threads.fork_from_turn = profile.fork;
    caps.turns.native_turn_id = true;
    caps.turns.emits_turn_started = true;
    caps.turns.emits_turn_completed = true;
    caps.turns.interrupt = true;
    caps.turns.active_steering = profile.native_steering;
    caps.turns.steering_by_interrupt_restart = true;
    caps.turns.queued_messages = true;
    caps.turns.terminal_status_quality = Quality::Strong;
    caps.streaming.assistant_text = true;
    caps.streaming.reasoning = true;
    caps.streaming.tool_output = true;
    caps.streaming.emits_message_completed = true;
    caps.tools.tool_item_ids = true;
    caps.tools.emits_tool_started = true;
    caps.tools.emits_tool_completed = true;
    caps.tools.emits_tool_output = true;
    caps.approvals.command = true;
    caps.approvals.native_request_ids = true;
    caps.approvals.callbacks_are_live_only = true;
    caps.planning.emits_plan_updated = true;
    caps.planning.emits_todo_list = true;
    caps.planning.structured_questions = true;
    caps.subagents.supported = true;
    caps.subagents.emits_lifecycle = true;
    caps.subagents.exposes_thread_ids = true;
    caps.subagents.can_close = true;
    caps.context.accepts_synthetic_user_context = true;
    caps.context.can_consume_handoff_summaries = true;
    caps.context.delta_handoff = true;
    caps.context.full_thread_handoff = true;
    caps.checkpointing.app_can_checkpoint_filesystem = true;
    caps.checkpointing.nested_scopes = true;
    caps.checkpointing.provider_can_rollback_conversation = profile.rollback;
    caps.identity.thread_ids = RefStrength::Strong;
    caps.identity.turn_ids = RefStrength::Strong;
    caps.identity.item_ids = RefStrength::Strong;
    caps.identity.request_ids = RefStrength::Strong;
    caps.runtime_policy.enforcement = Enforcement::Native;
    caps.runtime_policy.supported_modes = vec![
        RuntimeMode::Ask,
        RuntimeMode::AutoAcceptEdits,
        RuntimeMode::Auto,
        RuntimeMode::FullAccess,
    ];
    caps.extras.compaction = true;
    caps.extras.background_work = profile.background_work;
    caps.extras.model_discovery = true;
    caps
}

#[derive(Clone, Copy, Debug, Default)]
pub struct FakeDriver;

#[async_trait]
impl DriverV2 for FakeDriver {
    fn kind(&self) -> DriverFamily {
        DriverFamily::Test
    }

    fn capabilities(&self, cfg: &InstanceConfig) -> Capabilities {
        fake_capabilities(FakeProfile::from_config(cfg))
    }

    fn plan_selection_transition(
        &self,
        _session_caps: &Capabilities,
        from: &Selection,
        to: &Selection,
    ) -> TransitionPlan {
        let mut plans = Vec::new();
        if from.model != to.model {
            plans.push(TransitionPlan::ApplyInSession);
        }
        if from.reasoning_effort != to.reasoning_effort
            || from.service_tier != to.service_tier
            || from.context_window != to.context_window
            || from.agent_preset != to.agent_preset
        {
            plans.push(TransitionPlan::ApplyOnNextTurn);
        }
        if from.runtime_mode != to.runtime_mode {
            plans.push(TransitionPlan::RestartSession);
        }
        if from.interaction_mode != to.interaction_mode {
            plans.push(TransitionPlan::Reject {
                reason: "the fake provider has no plan mode".to_owned(),
            });
        }
        TransitionPlan::combine(plans)
    }

    async fn open_session(&self, ctx: SessionContext) -> DriverResult<OpenedSession> {
        let capabilities = self.capabilities(&ctx.instance);
        let spec = ProcessSpec {
            program: ctx.instance.binary.clone(),
            args: vec!["--jsonl".to_owned()],
            env: ctx.instance.env.clone(),
            cwd: ctx.cwd.clone(),
        };
        let transport = ctx.transports.spawn_stdio("main", spec).await?;
        let (events, receiver) = adapter_channel();
        let shared = Arc::new(Shared {
            session_id: ctx.provider_session_id,
            thread_id: ctx.thread_id,
            ids: ctx.ids.clone(),
            clock: ctx.clock.clone(),
            raw_log: ctx.raw_log.clone(),
            cwd: ctx.cwd.display().to_string(),
            writer: tokio::sync::Mutex::new(Some(transport.stdin)),
            next_id: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
            state: Mutex::new(State {
                starting: None,
                turns: HashMap::new(),
                texts: HashMap::new(),
                requests: HashMap::new(),
                coalescer: DeltaCoalescer::new(ctx.clock.clone()),
            }),
            events: Mutex::new(Some(events)),
            capabilities: capabilities.clone(),
        });
        tokio::spawn(read_loop(shared.clone(), transport.stdout, transport.exit));
        shared
            .request("initialize", json!({ "client": "waku", "cwd": shared.cwd }))
            .await?;
        Ok(OpenedSession {
            session: Arc::new(FakeSession { shared }),
            events: receiver,
            capabilities,
        })
    }
}

struct PendingTurn {
    run: RunRef,
    provider_thread_id: ProviderThreadId,
    ordinal: u32,
    /// Interrupted before the agent registered the turn.
    cancelled: bool,
}

struct OpenTurn {
    /// An interrupt arrived before the agent named the turn.
    cancel_requested: bool,
    run: RunRef,
    reference: ProviderTurnRef,
    provider_thread_id: ProviderThreadId,
    started_at: Timestamp,
}

struct State {
    starting: Option<PendingTurn>,
    turns: HashMap<String, OpenTurn>,
    texts: HashMap<String, String>,
    requests: HashMap<String, RuntimeRequest>,
    coalescer: DeltaCoalescer,
}

struct Shared {
    session_id: ProviderSessionId,
    thread_id: ThreadId,
    ids: Arc<dyn IdAllocator>,
    clock: Arc<dyn Clock>,
    raw_log: Arc<dyn RawFrameLog>,
    cwd: String,
    writer: tokio::sync::Mutex<Option<Box<dyn AsyncWrite + Send + Unpin>>>,
    next_id: AtomicU64,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, Value>>>>,
    state: Mutex<State>,
    events: Mutex<Option<AdapterEventSender>>,
    capabilities: Capabilities,
}

impl Shared {
    async fn write(&self, frame: &Value) -> DriverResult<()> {
        let mut line = serde_json::to_vec(frame).map_err(|error| DriverError::Protocol {
            reason: ProtocolReason::Decode,
            detail: error.to_string(),
        })?;
        line.push(b'\n');
        let mut writer = self.writer.lock().await;
        let Some(writer) = writer.as_mut() else {
            return Err(DriverError::SessionLost {
                detail: "stdin closed".to_owned(),
            });
        };
        writer.write_all(&line).await?;
        writer.flush().await?;
        Ok(())
    }

    async fn request(&self, method: &str, params: Value) -> DriverResult<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().insert(id, sender);
        self.write(&json!({ "id": id, "method": method, "params": params }))
            .await?;
        match receiver.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => Err(protocol_error(method, &error)),
            Err(_) => Err(DriverError::SessionLost {
                detail: format!("{method}: the agent exited"),
            }),
        }
    }

    async fn emit(&self, events: Vec<AdapterEvent>) {
        let Some(sender) = self.events.lock().clone() else {
            return;
        };
        for event in events {
            if sender.send(event).await.is_err() {
                return;
            }
        }
    }

    fn now(&self) -> Timestamp {
        self.clock.now()
    }

    fn bind<T: From<uuid::Uuid>>(&self, scope: uuid::Uuid, kind: EntityKind, native: &str) -> T {
        T::from(self.ids.bind(scope, kind, native))
    }

    fn turn_snapshot(&self, turn: &OpenTurn, status: ProviderTurnStatus) -> ProviderTurn {
        let terminal = !matches!(
            status,
            ProviderTurnStatus::Running | ProviderTurnStatus::Pending
        );
        ProviderTurn {
            id: turn.reference.provider_turn_id,
            provider_thread_id: turn.provider_thread_id,
            node_id: turn.run.root_node_id,
            run_attempt_id: Some(turn.run.attempt_id),
            native_turn_ref: Some(
                turn.reference
                    .native
                    .to_provider_ref(orch::DriverKind::Unknown),
            ),
            ordinal: turn.reference.ordinal,
            status,
            started_at: Some(turn.started_at),
            completed_at: terminal.then(|| self.now()),
            token_usage: None,
            turn_token_usage: None,
        }
    }
}

fn protocol_error(method: &str, error: &Value) -> DriverError {
    let code = error
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match code {
        "turn_settled" | "no_active_turn" => DriverError::SteerRejected {
            reason: if code == "turn_settled" {
                SteerRejectReason::TurnSettled
            } else {
                SteerRejectReason::NoActiveTurn
            },
        },
        "busy" => DriverError::Protocol {
            reason: ProtocolReason::PreviousTurnActive,
            detail: format!("{method}: {error}"),
        },
        "usage_limit" => DriverError::Provider(failure_from(error)),
        _ => DriverError::Protocol {
            reason: ProtocolReason::UnexpectedFrame,
            detail: format!("{method}: {error}"),
        },
    }
}

/// A classified failure from the agent's error object. The reset time comes
/// from the structured `resetsAt` field, never from the text.
fn failure_from(error: &Value) -> ProviderFailure {
    let code = error
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("The provider failed.");
    let class = match code {
        "usage_limit" => FailureClass::UsageLimit,
        "permission" => FailureClass::PermissionError,
        "invalid" => FailureClass::ValidationError,
        "provider" => FailureClass::ProviderError,
        _ => FailureClass::Unknown,
    };
    let mut failure = ProviderFailure::new(class, message);
    if !code.is_empty() {
        failure = failure.with_code(code);
    }
    failure.reset_at = error.get("resetsAt").and_then(Value::as_i64);
    failure.retryable = Some(class == FailureClass::UsageLimit);
    failure
}

async fn read_loop(
    shared: Arc<Shared>,
    stdout: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
    exit: futures_util::future::BoxFuture<'static, std::io::Result<ProcessExit>>,
) {
    let mut lines = BufReader::new(stdout).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(frame) = serde_json::from_str::<Value>(&line) else {
            shared
                .raw_log
                .record("main", FrameDirection::Inbound, &line);
            continue;
        };
        handle_frame(&shared, frame).await;
    }
    let status = exit.await;
    // Every request in flight fails; every open turn gets its one terminal.
    shared.pending.lock().clear();
    let events = {
        let mut state = shared.state.lock();
        let mut events = state.coalescer.flush();
        let detail = match &status {
            Ok(exit) if exit.success => "The provider process exited.".to_owned(),
            Ok(exit) => format!("The provider process exited ({:?}).", exit.code),
            Err(error) => format!("The provider process failed: {error}"),
        };
        let mut open = state
            .turns
            .drain()
            .map(|(_, turn)| turn)
            .collect::<Vec<_>>();
        open.sort_by_key(|turn| turn.reference.ordinal);
        for turn in open {
            events.push(AdapterEvent::TurnTerminal {
                terminal: TurnTerminal {
                    run: turn.run,
                    provider_turn: Some(turn.reference.clone()),
                    outcome: TurnOutcome::Failed {
                        failure: ProviderFailure::new(FailureClass::TransportError, detail.clone())
                            .with_code("process_exit"),
                    },
                    thread_disposition: ThreadDisposition::Reusable,
                    child_turns_continue: false,
                    background_pending: false,
                    summary: None,
                },
            });
        }
        // A start still in flight fails with `SessionLost` instead: only an
        // accepted turn gets a terminal.
        state.starting = None;
        events
    };
    shared.emit(events).await;
    // Dropping the last sender ends the session's event stream.
    shared.events.lock().take();
    shared.writer.lock().await.take();
}

async fn handle_frame(shared: &Arc<Shared>, frame: Value) {
    let method = frame
        .get("method")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let id = frame.get("id").cloned();
    match (method, id) {
        (None, Some(Value::Number(id))) => {
            let Some(id) = id.as_u64() else { return };
            let reply = match frame.get("error") {
                Some(error) => Err(error.clone()),
                None => Ok(frame.get("result").cloned().unwrap_or(Value::Null)),
            };
            if let Some(sender) = shared.pending.lock().remove(&id) {
                let _ = sender.send(reply);
            }
        }
        (Some(method), Some(id)) => {
            let native = match &id {
                Value::String(id) => id.clone(),
                other => other.to_string(),
            };
            let params = frame.get("params").cloned().unwrap_or(Value::Null);
            let events = server_request(shared, &method, &native, &params);
            if events.is_empty() {
                shared
                    .raw_log
                    .record("main", FrameDirection::Inbound, &frame.to_string());
            }
            shared.emit(events).await;
        }
        (Some(method), None) => {
            let params = frame.get("params").cloned().unwrap_or(Value::Null);
            let events = notification(shared, &method, &params);
            match events {
                Some(events) => shared.emit(events).await,
                None => shared
                    .raw_log
                    .record("main", FrameDirection::Inbound, &frame.to_string()),
            }
        }
        _ => shared
            .raw_log
            .record("main", FrameDirection::Inbound, &frame.to_string()),
    }
}

fn str_param<'a>(params: &'a Value, key: &str) -> &'a str {
    params.get(key).and_then(Value::as_str).unwrap_or_default()
}

/// A run-scoped item snapshot for an open turn.
fn item(
    turn: &OpenTurn,
    id: TurnItemId,
    now: Timestamp,
    status: ItemStatus,
    kind: TurnItemKind,
) -> TurnItem {
    TurnItem {
        id,
        thread_id: turn.run.thread_id,
        run_id: Some(turn.run.run_id),
        node_id: Some(turn.run.root_node_id),
        provider_thread_id: Some(turn.provider_thread_id),
        provider_turn_id: Some(turn.reference.provider_turn_id),
        native_item_ref: None,
        parent_item_id: None,
        position: 0,
        status,
        title: None,
        tool: None,
        stream_rev: 0,
        started_at: Some(now),
        completed_at: status.is_terminal().then_some(now),
        updated_at: now,
        kind,
    }
}

/// Normalizes a notification. `None` for an unknown frame.
fn notification(shared: &Shared, method: &str, params: &Value) -> Option<Vec<AdapterEvent>> {
    let now = shared.now();
    let mut state = shared.state.lock();
    let turn_id = str_param(params, "turnId").to_owned();
    match method {
        "turn/started" => {
            let pending = state.starting.take()?;
            let provider_turn_id = shared.bind(
                pending.provider_thread_id.0,
                EntityKind::ProviderTurn,
                &turn_id,
            );
            let turn = OpenTurn {
                cancel_requested: pending.cancelled,
                run: pending.run,
                reference: ProviderTurnRef {
                    provider_turn_id,
                    native: NativeRef::strong(&turn_id),
                    ordinal: pending.ordinal,
                },
                provider_thread_id: pending.provider_thread_id,
                started_at: now,
            };
            let snapshot = shared.turn_snapshot(&turn, ProviderTurnStatus::Running);
            state.turns.insert(turn_id, turn);
            Some(
                state
                    .coalescer
                    .offer(AdapterEvent::ProviderTurnUpdated { turn: snapshot }),
            )
        }
        "item/delta" | "item/completed" => {
            let turn = state.turns.get(&turn_id)?;
            let native_item = str_param(params, "itemId").to_owned();
            let reasoning = str_param(params, "kind") == "reasoning";
            let pt = turn.provider_thread_id.0;
            let item_id: TurnItemId = shared.bind(pt, EntityKind::Item, &native_item);
            let message_id: MessageId = shared.bind(pt, EntityKind::Message, &native_item);
            let completed = method == "item/completed";
            let status = if completed {
                ItemStatus::Completed
            } else {
                ItemStatus::Running
            };
            let text = if completed {
                str_param(params, "text").to_owned()
            } else {
                let text = state.texts.entry(native_item.clone()).or_default();
                text.push_str(str_param(params, "delta"));
                text.clone()
            };
            let turn = state.turns.get(&turn_id)?;
            let mut events = Vec::new();
            let streaming = !completed;
            if reasoning {
                events.push(AdapterEvent::TurnItemUpdated {
                    item: item(
                        turn,
                        item_id,
                        now,
                        status,
                        TurnItemKind::Reasoning { text, streaming },
                    ),
                });
            } else {
                events.push(AdapterEvent::MessageUpdated {
                    message: Message {
                        id: message_id,
                        thread_id: turn.run.thread_id,
                        run_id: Some(turn.run.run_id),
                        node_id: Some(turn.run.root_node_id),
                        role: MessageRole::Assistant,
                        created_by: Actor::Agent,
                        creation_source: CreationSource::Provider,
                        text: text.clone(),
                        display_text: None,
                        attachments: Vec::new(),
                        context: Vec::new(),
                        notification: None,
                        sender_thread_id: None,
                        streaming,
                        created_at: turn.started_at,
                        updated_at: now,
                    },
                });
                events.push(AdapterEvent::TurnItemUpdated {
                    item: item(
                        turn,
                        item_id,
                        now,
                        status,
                        TurnItemKind::AssistantMessage {
                            message_id,
                            text,
                            streaming,
                        },
                    ),
                });
            }
            if completed {
                state.texts.remove(&native_item);
            }
            Some(
                events
                    .into_iter()
                    .flat_map(|event| state.coalescer.offer(event))
                    .collect(),
            )
        }
        "tool/started" | "tool/completed" => {
            let turn = state.turns.get(&turn_id)?;
            let native_item = str_param(params, "itemId");
            let pt = turn.provider_thread_id.0;
            let item_id: TurnItemId = shared.bind(pt, EntityKind::Item, native_item);
            let node_id: NodeId = shared.bind(pt, EntityKind::Node, native_item);
            let completed = method == "tool/completed";
            let exit_code = params
                .get("exitCode")
                .and_then(Value::as_i64)
                .map(|code| code as i32);
            let mut row = item(
                turn,
                item_id,
                now,
                if completed {
                    ItemStatus::Completed
                } else {
                    ItemStatus::Running
                },
                TurnItemKind::CommandExecution {
                    input: str_param(params, "command").to_owned(),
                    output: params
                        .get("output")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    output_truncated: false,
                    output_omitted: false,
                    output_indicates_failure: exit_code.is_some_and(|code| code != 0),
                    exit_code,
                },
            );
            row.node_id = Some(node_id);
            row.native_item_ref = Some(ProviderRef::strong(orch::DriverKind::Unknown, native_item));
            let node = ExecutionNode {
                id: node_id,
                thread_id: turn.run.thread_id,
                run_id: Some(turn.run.run_id),
                parent_node_id: Some(turn.run.root_node_id),
                root_node_id: turn.run.root_node_id,
                kind: NodeKind::ToolCall,
                status: if completed {
                    NodeStatus::Completed
                } else {
                    NodeStatus::Running
                },
                counts_for_run: false,
                provider_thread_id: Some(turn.provider_thread_id),
                provider_turn_id: Some(turn.reference.provider_turn_id),
                native_item_ref: row.native_item_ref.clone(),
                runtime_request_id: None,
                checkpoint_scope_id: None,
                started_at: Some(now),
                completed_at: completed.then_some(now),
            };
            let events = vec![
                AdapterEvent::NodeUpdated { node },
                AdapterEvent::TurnItemUpdated { item: row },
            ];
            Some(
                events
                    .into_iter()
                    .flat_map(|event| state.coalescer.offer(event))
                    .collect(),
            )
        }
        "tool/output" => {
            let turn = state.turns.get(&turn_id)?;
            let item_id: TurnItemId = shared.bind(
                turn.provider_thread_id.0,
                EntityKind::Item,
                str_param(params, "itemId"),
            );
            Some(vec![AdapterEvent::CommandOutputDelta {
                item: item_id,
                chunk: str_param(params, "chunk").as_bytes().to_vec(),
                replace: false,
            }])
        }
        "plan/updated" => {
            let turn = state.turns.get(&turn_id)?;
            let plan_id: PlanId = shared.bind(
                turn.provider_thread_id.0,
                EntityKind::Plan,
                str_param(params, "planId"),
            );
            let steps = params
                .get("steps")
                .and_then(Value::as_array)
                .map(|steps| {
                    steps
                        .iter()
                        .map(|step| PlanStep {
                            id: str_param(step, "id").to_owned(),
                            text: str_param(step, "text").to_owned(),
                            status: match str_param(step, "status") {
                                "completed" => PlanStepStatus::Completed,
                                "running" => PlanStepStatus::Running,
                                _ => PlanStepStatus::Pending,
                            },
                        })
                        .collect()
                })
                .unwrap_or_default();
            let plan = Plan {
                id: plan_id,
                thread_id: turn.run.thread_id,
                run_id: Some(turn.run.run_id),
                node_id: turn.run.root_node_id,
                status: PlanStatus::Active,
                kind: PlanKind::TodoList {
                    steps,
                    explanation: None,
                },
            };
            Some(state.coalescer.offer(AdapterEvent::PlanUpdated { plan }))
        }
        "subagent/updated" => {
            let turn = state.turns.get(&turn_id)?;
            let pt = turn.provider_thread_id.0;
            let task = str_param(params, "taskId");
            let node_id: NodeId = shared.bind(pt, EntityKind::Subagent, task);
            let status = match str_param(params, "status") {
                "completed" => SubagentStatus::Completed,
                "failed" => SubagentStatus::Failed,
                "cancelled" => SubagentStatus::Cancelled,
                _ => SubagentStatus::Running,
            };
            let child_thread = str_param(params, "threadId");
            let mut events = Vec::new();
            let child_provider_thread = (!child_thread.is_empty()).then(|| {
                let id: ProviderThreadId =
                    shared.bind(pt, EntityKind::ProviderThread, child_thread);
                id
            });
            if let Some(child) = child_provider_thread
                && status == SubagentStatus::Running
            {
                events.push(AdapterEvent::ProviderThreadUpdated {
                    thread: ProviderThread {
                        id: child,
                        driver: orch::DriverKind::Unknown,
                        provider_instance_id: ProviderInstanceId::new(FAKE_PROVIDER),
                        provider_session_id: Some(shared.session_id),
                        app_thread_id: Some(shared.thread_id),
                        owner_node_id: Some(node_id),
                        native_thread_ref: Some(ProviderRef::strong(
                            orch::DriverKind::Unknown,
                            child_thread,
                        )),
                        native_conversation_head_ref: None,
                        status: ProviderThreadStatus::Active,
                        first_run_ordinal: None,
                        last_run_ordinal: None,
                        handoff_ids: Vec::new(),
                        forked_from: None,
                        pending_background_tasks: Vec::new(),
                        context_usage: None,
                        goal: None,
                        native_metadata: None,
                        created_at: now,
                        updated_at: now,
                    },
                    retained_native_turn_ids: None,
                });
            }
            events.push(AdapterEvent::SubagentUpdated {
                subagent: Subagent {
                    id: node_id,
                    thread_id: turn.run.thread_id,
                    run_id: Some(turn.run.run_id),
                    parent_node_id: turn.run.root_node_id,
                    origin: SubagentOrigin::ProviderNative,
                    created_by: Actor::Agent,
                    driver: orch::DriverKind::Unknown,
                    provider_instance_id: ProviderInstanceId::new(FAKE_PROVIDER),
                    provider_thread_id: child_provider_thread,
                    child_thread_id: None,
                    native_task_ref: Some(ProviderRef::strong(orch::DriverKind::Unknown, task)),
                    prompt: str_param(params, "prompt").to_owned(),
                    title: None,
                    model: None,
                    status,
                    progress: None,
                    result: params
                        .get("result")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    completion_wake: None,
                    completion_delivery: None,
                    started_at: Some(turn.started_at),
                    completed_at: status.is_terminal().then_some(now),
                    updated_at: now,
                },
            });
            Some(
                events
                    .into_iter()
                    .flat_map(|event| state.coalescer.offer(event))
                    .collect(),
            )
        }
        "subagent/turn_completed" => {
            // A child turn: its own provider turn row, then a terminal that
            // names it. It never completes the parent run.
            let turn = state.turns.get(&turn_id)?;
            let pt = turn.provider_thread_id.0;
            let child_thread: ProviderThreadId = shared.bind(
                pt,
                EntityKind::ProviderThread,
                str_param(params, "threadId"),
            );
            let node_id: NodeId =
                shared.bind(pt, EntityKind::Subagent, str_param(params, "taskId"));
            let native_turn = str_param(params, "childTurnId");
            let child_turn_id: ProviderTurnId =
                shared.bind(child_thread.0, EntityKind::ProviderTurn, native_turn);
            let reference = ProviderTurnRef {
                provider_turn_id: child_turn_id,
                native: NativeRef::strong(native_turn),
                ordinal: 1,
            };
            let events = vec![
                AdapterEvent::ProviderTurnUpdated {
                    turn: ProviderTurn {
                        id: child_turn_id,
                        provider_thread_id: child_thread,
                        node_id,
                        run_attempt_id: None,
                        native_turn_ref: Some(
                            reference.native.to_provider_ref(orch::DriverKind::Unknown),
                        ),
                        ordinal: 1,
                        status: ProviderTurnStatus::Running,
                        started_at: Some(now),
                        completed_at: None,
                        token_usage: None,
                        turn_token_usage: None,
                    },
                },
                AdapterEvent::TurnTerminal {
                    terminal: TurnTerminal {
                        run: turn.run,
                        provider_turn: Some(reference),
                        outcome: TurnOutcome::Completed,
                        thread_disposition: ThreadDisposition::Reusable,
                        child_turns_continue: false,
                        background_pending: false,
                        summary: None,
                    },
                },
            ];
            Some(
                events
                    .into_iter()
                    .flat_map(|event| state.coalescer.offer(event))
                    .collect(),
            )
        }
        "turn/completed" => {
            let turn = state.turns.remove(&turn_id)?;
            let outcome = match str_param(params, "status") {
                "completed" => TurnOutcome::Completed,
                "interrupted" => TurnOutcome::Interrupted,
                "cancelled" => TurnOutcome::Cancelled,
                _ => TurnOutcome::Failed {
                    failure: failure_from(params.get("error").unwrap_or(&Value::Null)),
                },
            };
            let status = match &outcome {
                TurnOutcome::Completed => ProviderTurnStatus::Completed,
                TurnOutcome::Interrupted => ProviderTurnStatus::Interrupted,
                TurnOutcome::Cancelled => ProviderTurnStatus::Cancelled,
                TurnOutcome::Failed { .. } => ProviderTurnStatus::Failed,
            };
            // Flush before the terminal (RFC 0002 rule 7).
            let mut events = state.coalescer.flush();
            events.push(AdapterEvent::ProviderTurnUpdated {
                turn: shared.turn_snapshot(&turn, status),
            });
            events.push(AdapterEvent::TurnTerminal {
                terminal: TurnTerminal {
                    run: turn.run,
                    provider_turn: Some(turn.reference.clone()),
                    outcome,
                    thread_disposition: ThreadDisposition::Reusable,
                    child_turns_continue: params
                        .get("childTurnsContinue")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    background_pending: false,
                    summary: params
                        .get("summary")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                },
            });
            Some(events)
        }
        _ => None,
    }
}

/// Normalizes an agent → driver request (approvals and questions).
fn server_request(
    shared: &Shared,
    method: &str,
    native: &str,
    params: &Value,
) -> Vec<AdapterEvent> {
    let now = shared.now();
    let mut state = shared.state.lock();
    let turn_id = str_param(params, "turnId");
    let Some(turn) = state.turns.get(turn_id) else {
        return Vec::new();
    };
    let pt = turn.provider_thread_id.0;
    let request_id: RuntimeRequestId = shared.bind(pt, EntityKind::Request, native);
    let (kind, options, questions) = match method {
        "approval/request" => {
            let options = params
                .get("options")
                .and_then(Value::as_array)
                .map(|options| {
                    options
                        .iter()
                        .map(|option| PermissionOption {
                            id: str_param(option, "id").to_owned(),
                            label: str_param(option, "label").to_owned(),
                            allow: option
                                .get("allow")
                                .and_then(Value::as_bool)
                                .unwrap_or(false),
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            (RuntimeRequestKind::CommandApproval, options, Vec::new())
        }
        "question/request" => {
            let questions = params
                .get("questions")
                .and_then(Value::as_array)
                .map(|questions| {
                    questions
                        .iter()
                        .map(|question| UserInputQuestion {
                            id: str_param(question, "id").to_owned(),
                            header: str_param(question, "header").to_owned(),
                            question: str_param(question, "question").to_owned(),
                            options: question
                                .get("options")
                                .and_then(Value::as_array)
                                .map(|options| {
                                    options
                                        .iter()
                                        .map(|option| UserInputOption {
                                            label: str_param(option, "label").to_owned(),
                                            description: None,
                                        })
                                        .collect()
                                })
                                .unwrap_or_default(),
                            multi_select: false,
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            (RuntimeRequestKind::UserInput, Vec::new(), questions)
        }
        _ => return Vec::new(),
    };
    let request = RuntimeRequest {
        id: request_id,
        thread_id: turn.run.thread_id,
        node_id: turn.run.root_node_id,
        provider_turn_id: Some(turn.reference.provider_turn_id),
        native_request_ref: Some(ProviderRef::strong(orch::DriverKind::Unknown, native)),
        kind,
        status: RequestStatus::Pending,
        response_capability: ResponseCapability::Live {
            provider_session_id: shared.session_id,
        },
        options,
        questions,
        response: None,
        created_at: now,
        resolved_at: None,
    };
    let mut events = Vec::new();
    if kind == RuntimeRequestKind::CommandApproval {
        // The row carries the prompt; `decide_ingest` keeps it.
        events.push(AdapterEvent::TurnItemUpdated {
            item: item(
                turn,
                waku_orchestrator::provider::derived::request_item(request_id),
                now,
                ItemStatus::Waiting,
                TurnItemKind::ApprovalRequest {
                    request_id,
                    request_kind: kind,
                    prompt: Some(str_param(params, "command").to_owned()),
                    app_name: None,
                    options: request.options.clone(),
                },
            ),
        });
    }
    events.push(AdapterEvent::RuntimeRequestUpdated {
        request: request.clone(),
    });
    state.requests.insert(native.to_owned(), request);
    events
        .into_iter()
        .flat_map(|event| state.coalescer.offer(event))
        .collect()
}

struct FakeSession {
    shared: Arc<Shared>,
}

impl FakeSession {
    fn native_turn(&self, reference: Option<&ProviderTurnRef>, run: &RunRef) -> Option<String> {
        let state = self.shared.state.lock();
        reference
            .map(|reference| reference.native.id.clone())
            .filter(|id| state.turns.contains_key(id))
            .or_else(|| {
                state
                    .turns
                    .iter()
                    .find(|(_, turn)| turn.run.attempt_id == run.attempt_id)
                    .map(|(id, _)| id.clone())
            })
    }
}

#[async_trait]
impl DriverSession for FakeSession {
    fn capabilities(&self) -> &Capabilities {
        &self.shared.capabilities
    }

    async fn ensure_thread(&self, req: EnsureThread) -> DriverResult<ProviderThreadRef> {
        let result = self
            .shared
            .request("thread/start", json!({ "cwd": self.shared.cwd }))
            .await?;
        let native = str_param(&result, "threadId").to_owned();
        let provider_thread_id = match &req.existing {
            Some(existing) => existing.provider_thread_id,
            None => self.shared.bind(
                self.shared.session_id.0,
                EntityKind::ProviderThread,
                &native,
            ),
        };
        Ok(ProviderThreadRef {
            provider_thread_id,
            native: NativeRef::strong(native),
            cursor: None,
        })
    }

    async fn resume_thread(&self, req: ResumeThread) -> DriverResult<ProviderThreadRef> {
        self.shared
            .request("thread/resume", json!({ "threadId": req.thread.native.id }))
            .await?;
        Ok(req.thread)
    }

    async fn start_turn(&self, req: TurnStart) -> DriverResult<TurnAccepted> {
        {
            let mut state = self.shared.state.lock();
            if state.starting.is_some() || !state.turns.is_empty() {
                return Err(DriverError::NotReady(NotReadyReason::PreviousTurnActive));
            }
            state.starting = Some(PendingTurn {
                run: req.run,
                provider_thread_id: req.thread.provider_thread_id,
                ordinal: req.provider_turn_ordinal,
                cancelled: false,
            });
        }
        let text = wrap_synthetic_context(&req.app_context, &req.message.text);
        let mut params = json!({ "threadId": req.thread.native.id, "text": text });
        if let Some(model) = &req.selection.model {
            params["model"] = json!(model);
        }
        let result = self.shared.request("turn/start", params).await;
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                self.shared.state.lock().starting = None;
                return Err(error);
            }
        };
        let native = str_param(&result, "turnId").to_owned();
        let cancelled = {
            let state = self.shared.state.lock();
            state
                .starting
                .as_ref()
                .is_some_and(|pending| pending.cancelled)
                || state
                    .turns
                    .get(&native)
                    .is_some_and(|turn| turn.cancel_requested)
        };
        if cancelled {
            // Interrupted while the start was in flight: the agent knows the
            // turn now, so interrupt it for real.
            self.shared
                .request(
                    "turn/interrupt",
                    json!({ "threadId": req.thread.native.id, "turnId": native }),
                )
                .await?;
        }
        Ok(TurnAccepted {
            provider_turn: Some(ProviderTurnRef {
                provider_turn_id: self.shared.bind(
                    req.thread.provider_thread_id.0,
                    EntityKind::ProviderTurn,
                    &native,
                ),
                native: NativeRef::strong(native),
                ordinal: req.provider_turn_ordinal,
            }),
        })
    }

    async fn steer_turn(&self, req: Steer) -> DriverResult<()> {
        let Some(turn) = self.native_turn(req.provider_turn.as_ref(), &req.run) else {
            return Err(DriverError::SteerRejected {
                reason: SteerRejectReason::NoActiveTurn,
            });
        };
        self.shared
            .request(
                "turn/steer",
                json!({ "threadId": req.thread.native.id, "turnId": turn, "text": req.message.text }),
            )
            .await
            .map(|_| ())
    }

    async fn interrupt_turn(&self, req: Interrupt) -> DriverResult<()> {
        match self.native_turn(req.provider_turn.as_ref(), &req.run) {
            Some(turn) => self
                .shared
                .request(
                    "turn/interrupt",
                    json!({ "threadId": req.thread.native.id, "turnId": turn }),
                )
                .await
                .map(|_| ()),
            None => {
                // Before the agent registered the turn: `start_turn` sends
                // the interrupt as soon as the agent names the turn, and
                // that turn's terminal is the run's one terminal.
                if let Some(pending) = self.shared.state.lock().starting.as_mut()
                    && pending.run.attempt_id == req.run.attempt_id
                {
                    pending.cancelled = true;
                }
                Ok(())
            }
        }
    }

    async fn respond(&self, req: RuntimeResponse) -> DriverResult<()> {
        let result = match &req.reply {
            RuntimeReply::Decision { option_id } => json!({ "optionId": option_id }),
            RuntimeReply::Answers(answers) => json!({
                "answers": answers
                    .iter()
                    .map(|answer| (answer.question_id.clone(), json!(answer.answers)))
                    .collect::<serde_json::Map<_, _>>()
            }),
            RuntimeReply::Dismiss => json!({ "dismissed": true }),
        };
        self.shared
            .write(&json!({ "id": req.native.id, "result": result }))
            .await?;
        let now = self.shared.now();
        let resolved = self
            .shared
            .state
            .lock()
            .requests
            .remove(&req.native.id)
            .map(|request| RuntimeRequest {
                status: RequestStatus::Resolved,
                resolved_at: Some(now),
                ..request
            });
        if let Some(request) = resolved {
            self.shared
                .emit(vec![AdapterEvent::RuntimeRequestUpdated { request }])
                .await;
        }
        Ok(())
    }

    async fn read_snapshot(&self, _thread: &ProviderThreadRef) -> DriverResult<ThreadSnapshot> {
        Err(DriverError::Unsupported {
            operation: "read_snapshot",
        })
    }

    async fn rollback(&self, req: Rollback) -> DriverResult<RollbackOutcome> {
        if !self.shared.capabilities.threads.rollback {
            return Err(DriverError::Unsupported {
                operation: "rollback",
            });
        }
        let result = self
            .shared
            .request(
                "thread/rollback",
                json!({ "threadId": req.thread.native.id, "numTurns": req.turns_to_remove }),
            )
            .await?;
        let native = str_param(&result, "threadId").to_owned();
        let replaced = native != req.thread.native.id;
        let thread = if replaced {
            ProviderThreadRef {
                provider_thread_id: self.shared.bind(
                    req.thread.provider_thread_id.0,
                    EntityKind::ProviderThread,
                    &native,
                ),
                native: NativeRef::strong(native),
                cursor: None,
            }
        } else {
            req.thread
        };
        Ok(RollbackOutcome {
            thread,
            replaced,
            snapshot: None,
            retained_native_turn_ids: None,
        })
    }

    async fn fork(&self, req: ForkRequest) -> DriverResult<ProviderThreadRef> {
        if !self.shared.capabilities.threads.fork {
            return Err(DriverError::Unsupported { operation: "fork" });
        }
        let mut params = json!({ "threadId": req.source.native.id });
        if let ForkPoint::AfterProviderTurn { turn, .. } = &req.at {
            params["afterTurnId"] = json!(turn.native.id);
        }
        let result = self.shared.request("thread/fork", params).await?;
        let native = str_param(&result, "threadId").to_owned();
        Ok(ProviderThreadRef {
            provider_thread_id: self.shared.bind(
                req.source.provider_thread_id.0,
                EntityKind::ProviderThread,
                &native,
            ),
            native: NativeRef::strong(native),
            cursor: None,
        })
    }

    async fn apply_selection(&self, to: &Selection) -> DriverResult<()> {
        self.shared
            .request("session/set_model", json!({ "model": to.model }))
            .await
            .map(|_| ())
    }

    async fn compact(&self, thread: &ProviderThreadRef) -> DriverResult<()> {
        self.shared
            .request("thread/compact", json!({ "threadId": thread.native.id }))
            .await
            .map(|_| ())
    }

    async fn stop_subagent(&self, subagent: SubagentRef) -> DriverResult<SubagentStopOutcome> {
        self.shared
            .request(
                "subagent/stop",
                json!({ "taskId": subagent.native_task_id }),
            )
            .await
            .map(|_| SubagentStopOutcome::Stopped)
    }

    fn has_pending_background_work(&self) -> bool {
        self.shared.capabilities.extras.background_work
    }

    async fn close(&self) -> DriverResult<()> {
        // Closing stdin is the shutdown signal; the agent exits on EOF.
        if let Some(mut writer) = self.shared.writer.lock().await.take() {
            let _ = writer.shutdown().await;
        }
        Ok(())
    }
}

//! One ACP session: the connection, the read loop and the normalization of
//! ACP frames into `AdapterEvent`s (RFC 0002 "ACP family").
//!
//! Rules this file keeps:
//! - every `start_turn` that returns `Ok` produces exactly one
//!   `TurnTerminal`: from the `session/prompt` response, Grok's
//!   `_x.ai/session/prompt_complete`, an interrupt before the prompt was sent,
//!   or the agent exiting;
//! - text and reasoning go through the `DeltaCoalescer`;
//! - unknown frames go to the raw log and are otherwise ignored;
//! - native option ids pass through unchanged.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use agent_client_protocol::schema::v1::{SessionConfigOption, SessionModeState};
use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use waku_protocol::model::{ProviderResumeCursor, ReportedCommand};
use waku_protocol::orchestration::*;

use super::flavor::{self, AgentInfo, Flavor};
use super::protocol::{Connection, Inbound, METHOD_NOT_FOUND, RpcError, decode};
use super::questions::{QuestionKind, permission_reason};
use super::selection::{self, ModelChoice};
use crate::driver::v2::coalescer::DeltaCoalescer;
use crate::driver::v2::{
    AdapterEvent, AdapterEventSender, Clock, DriverError, DriverResult, DriverSession,
    EnsureThread, EntityKind, ForkPoint, ForkRequest, FrameDirection, IdAllocator, Interrupt,
    NativeRef, NotReadyReason, ProcessExit, ProtocolReason, ProviderThreadRef, ProviderTurnRef,
    RawFrameLog, ResumeThread, Rollback, RollbackOutcome, RollbackTarget, RunRef, RuntimeReply,
    RuntimeResponse, Selection, Steer, ThreadDisposition, ThreadSnapshot, TransportFactory,
    TurnAccepted, TurnOutcome, TurnStart, TurnTerminal, wrap_synthetic_context,
};

const CHANNEL: &str = "main";
const STDERR_LINES: usize = 128;

/// Where provider files live for this session (Grok sessions, Kimi wire logs).
pub struct Homes {
    pub grok: PathBuf,
    pub kimi: PathBuf,
}

/// Fields the session reads but never changes.
pub struct Setup {
    pub flavor: Flavor,
    pub provider_session_id: ProviderSessionId,
    pub thread_id: ThreadId,
    pub instance_id: ProviderInstanceId,
    pub cwd: PathBuf,
    pub binary: PathBuf,
    pub homes: Homes,
    pub transports: Arc<dyn TransportFactory>,
    pub ids: Arc<dyn IdAllocator>,
    pub clock: Arc<dyn Clock>,
    pub raw_log: Arc<dyn RawFrameLog>,
}

/// The ACP session currently loaded on the connection.
struct Active {
    id: String,
    config_options: Vec<SessionConfigOption>,
    /// The model choice last applied, so a turn re-applies only a change.
    applied: ModelChoice,
}

/// A text or reasoning block of the running turn.
struct Segment {
    item: TurnItemId,
    message: Option<MessageId>,
    text: String,
}

struct ToolState {
    item: TurnItemId,
    node: NodeId,
    kind: String,
    title: String,
    input: Value,
    started_at: Timestamp,
}

struct OpenTurn {
    run: RunRef,
    reference: ProviderTurnRef,
    provider_thread_id: ProviderThreadId,
    native_session: String,
    started_at: Timestamp,
    rpc_id: u64,
    /// Grok's `_meta.promptId` for `_x.ai/session/prompt_complete`.
    prompt_id: Option<String>,
    /// Waku sent `session/cancel` for this turn.
    interrupted: bool,
    produced_content: bool,
    /// Kimi's wire-log length before the prompt.
    kimi_offset: Option<u64>,
    next_item: u64,
    text: Option<Segment>,
    reasoning: Option<Segment>,
    tools: HashMap<String, ToolState>,
    usage: Option<TokenUsage>,
}

struct Starting {
    attempt: RunAttemptId,
    cancelled: bool,
}

enum PendingKind {
    Permission,
    Question { kind: QuestionKind, params: Value },
}

struct PendingRequest {
    id: Value,
    kind: PendingKind,
    request: RuntimeRequest,
}

struct State {
    active: Option<Active>,
    /// Applied when a session is created or activated.
    selection: Selection,
    turn: Option<OpenTurn>,
    starting: Option<Starting>,
    /// Session updates during a `session/load` replay are not live events.
    suppress_updates: bool,
    capture: Option<Vec<Value>>,
    coalescer: DeltaCoalescer,
    requests: HashMap<String, PendingRequest>,
}

pub struct Shared {
    pub setup: Setup,
    /// What `initialize` returned and the capabilities derived from it.
    negotiated: std::sync::OnceLock<(AgentInfo, Capabilities)>,
    connection: Connection,
    events: Mutex<Option<AdapterEventSender>>,
    state: Mutex<State>,
    stderr: Mutex<VecDeque<String>>,
    /// Serializes session-level operations (bind, start, rollback, fork).
    op: tokio::sync::Mutex<()>,
    prompts: AtomicU64,
}

fn native_key(id: &Value) -> String {
    match id {
        Value::String(id) => id.clone(),
        other => other.to_string(),
    }
}

fn str_at<'a>(value: &'a Value, pointer: &str) -> Option<&'a str> {
    value.pointer(pointer).and_then(Value::as_str)
}

impl Shared {
    pub fn new(
        setup: Setup,
        connection: Connection,
        events: AdapterEventSender,
        selection: Selection,
    ) -> Self {
        let coalescer = DeltaCoalescer::new(setup.clock.clone());
        Self {
            setup,
            negotiated: std::sync::OnceLock::new(),
            connection,
            events: Mutex::new(Some(events)),
            state: Mutex::new(State {
                active: None,
                selection,
                turn: None,
                starting: None,
                suppress_updates: false,
                capture: None,
                coalescer,
                requests: HashMap::new(),
            }),
            stderr: Mutex::new(VecDeque::new()),
            op: tokio::sync::Mutex::new(()),
            prompts: AtomicU64::new(1),
        }
    }

    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Records the handshake's result (once, right after `initialize`).
    pub fn negotiated(&self, agent: AgentInfo, capabilities: Capabilities) {
        let _ = self.negotiated.set((agent, capabilities));
    }

    fn agent(&self) -> &AgentInfo {
        static NONE: std::sync::OnceLock<AgentInfo> = std::sync::OnceLock::new();
        self.negotiated
            .get()
            .map_or_else(|| NONE.get_or_init(AgentInfo::default), |(agent, _)| agent)
    }

    pub fn caps(&self) -> &Capabilities {
        static NONE: std::sync::OnceLock<Capabilities> = std::sync::OnceLock::new();
        self.negotiated
            .get()
            .map_or_else(|| NONE.get_or_init(Capabilities::default), |(_, caps)| caps)
    }

    fn flavor(&self) -> Flavor {
        self.setup.flavor
    }

    fn now(&self) -> Timestamp {
        self.setup.clock.now()
    }

    fn bind<T: From<uuid::Uuid>>(&self, scope: uuid::Uuid, kind: EntityKind, native: &str) -> T {
        T::from(self.setup.ids.bind(scope, kind, native))
    }

    fn bind_ordinal<T: From<uuid::Uuid>>(&self, scope: uuid::Uuid, kind: EntityKind, n: u64) -> T {
        T::from(self.setup.ids.bind_ordinal(scope, kind, n))
    }

    fn note(&self, frame: &str) {
        self.setup
            .raw_log
            .record(CHANNEL, FrameDirection::Inbound, frame);
    }

    async fn emit(&self, events: Vec<AdapterEvent>) {
        if events.is_empty() {
            return;
        }
        let Some(sender) = self.events.lock().clone() else {
            return;
        };
        for event in events {
            if sender.send(event).await.is_err() {
                return;
            }
        }
    }

    fn cursor(&self, native: &str) -> Option<ProviderResumeCursor> {
        self.setup
            .flavor
            .provider_kind()
            .map(|provider| ProviderResumeCursor::from_session_id(provider, native.to_owned()))
    }

    pub fn push_stderr(&self, line: String) {
        if line.trim().is_empty() {
            return;
        }
        let mut lines = self.stderr.lock();
        if lines.len() == STDERR_LINES {
            lines.pop_front();
        }
        lines.push_back(line);
    }

    fn stderr_error(&self) -> Option<String> {
        crate::driver::support::provider_stderr_error(self.stderr.lock().iter().cloned().collect())
    }

    // -- sessions -----------------------------------------------------------------

    /// Records a session the agent created or loaded and applies the access
    /// mode and model choice to it.
    async fn activate(&self, id: String, response: &Value) {
        let config_options: Vec<SessionConfigOption> = response
            .get("configOptions")
            .cloned()
            .and_then(|options| serde_json::from_value(options).ok())
            .unwrap_or_default();
        let modes: Option<SessionModeState> = response
            .get("modes")
            .cloned()
            .and_then(|modes| serde_json::from_value(modes).ok());
        let selection = self.state.lock().selection.clone();
        if let Some(mode) =
            selection::desired_access_mode(self.flavor(), modes.as_ref(), selection.runtime_mode)
        {
            // Mode selection is opportunistic: an agent can advertise a mode
            // but reject a later transition without invalidating the session.
            let _ = self
                .connection
                .request(
                    "session/set_mode",
                    json!({ "sessionId": id, "modeId": mode }),
                )
                .await;
        }
        let choice = ModelChoice::from_selection(&selection);
        if let Err(error) = selection::apply_model(
            &self.connection,
            self.flavor(),
            &id,
            &config_options,
            &choice,
        )
        .await
        {
            self.model_error(&error);
        }
        self.state.lock().active = Some(Active {
            id: id.clone(),
            config_options,
            applied: choice,
        });
    }

    fn model_error(&self, error: &str) {
        self.setup.raw_log.record(
            CHANNEL,
            FrameDirection::Note,
            &format!("model selection failed: {error}"),
        );
    }

    /// `session/new`.
    pub async fn new_session(&self) -> DriverResult<String> {
        let response = self
            .connection
            .request(
                "session/new",
                json!({ "cwd": self.setup.cwd, "mcpServers": [] }),
            )
            .await
            .map_err(|error| self.request_error("session/new", error))?;
        let id = response
            .get("sessionId")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| DriverError::Protocol {
                reason: ProtocolReason::UnexpectedFrame,
                detail: "session/new returned no sessionId".to_owned(),
            })?
            .to_owned();
        self.activate(id.clone(), &response).await;
        Ok(id)
    }

    /// `session/resume` if advertised, else `session/load` with its replay
    /// suppressed. `None` when the agent could not reopen it.
    async fn reopen(&self, id: &str) -> Option<Value> {
        if self.agent().resume
            && let Ok(response) = self
                .connection
                .request(
                    "session/resume",
                    json!({ "sessionId": id, "cwd": self.setup.cwd }),
                )
                .await
        {
            return Some(response);
        }
        if self.agent().load_session {
            self.state.lock().suppress_updates = true;
            let response = self
                .connection
                .request(
                    "session/load",
                    json!({ "sessionId": id, "cwd": self.setup.cwd, "mcpServers": [] }),
                )
                .await;
            self.state.lock().suppress_updates = false;
            if let Ok(response) = response {
                return Some(response);
            }
        }
        None
    }

    /// Makes `native` the loaded session. Returns the native id actually
    /// loaded: a session the agent cannot reopen is replaced by a new one, as
    /// the v1 driver did.
    async fn ensure_active(&self, native: &str) -> DriverResult<String> {
        if native.is_empty() {
            return self.new_session().await;
        }
        if self
            .state
            .lock()
            .active
            .as_ref()
            .is_some_and(|active| active.id == native)
        {
            return Ok(native.to_owned());
        }
        match self.reopen(native).await {
            Some(response) => {
                self.activate(native.to_owned(), &response).await;
                Ok(native.to_owned())
            }
            None => self.new_session().await,
        }
    }

    fn request_error(&self, method: &str, error: RpcError) -> DriverError {
        if error.is_closed() {
            return DriverError::SessionLost {
                detail: self
                    .stderr_error()
                    .unwrap_or_else(|| format!("{method}: the agent exited")),
            };
        }
        DriverError::Provider(flavor::classify_rpc_failure(
            self.flavor(),
            &error,
            self.now(),
        ))
    }

    fn thread_ref(&self, provider_thread_id: ProviderThreadId, native: &str) -> ProviderThreadRef {
        ProviderThreadRef {
            provider_thread_id,
            native: NativeRef::strong(native),
            cursor: self.cursor(native),
        }
    }

    // -- turns --------------------------------------------------------------------

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
            native_turn_ref: None,
            ordinal: turn.reference.ordinal,
            status,
            started_at: Some(turn.started_at),
            completed_at: terminal.then(|| self.now()),
            token_usage: turn.usage.clone(),
            turn_token_usage: None,
        }
    }

    fn item(
        &self,
        turn: &OpenTurn,
        id: TurnItemId,
        status: ItemStatus,
        kind: TurnItemKind,
    ) -> TurnItem {
        let now = self.now();
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
            started_at: Some(turn.started_at),
            completed_at: status.is_terminal().then_some(now),
            updated_at: now,
            kind,
        }
    }

    fn message(&self, turn: &OpenTurn, id: MessageId, text: &str, streaming: bool) -> Message {
        Message {
            id,
            thread_id: turn.run.thread_id,
            run_id: Some(turn.run.run_id),
            node_id: Some(turn.run.root_node_id),
            role: MessageRole::Assistant,
            created_by: Actor::Agent,
            creation_source: CreationSource::Provider,
            text: text.to_owned(),
            display_text: None,
            attachments: Vec::new(),
            context: Vec::new(),
            notification: None,
            sender_thread_id: None,
            streaming,
            created_at: turn.started_at,
            updated_at: self.now(),
        }
    }

    /// Snapshots of a text or reasoning segment.
    fn segment_events(&self, turn: &OpenTurn, segment: &Segment, done: bool) -> Vec<AdapterEvent> {
        let status = if done {
            ItemStatus::Completed
        } else {
            ItemStatus::Running
        };
        let streaming = !done;
        match segment.message {
            Some(message_id) => vec![
                AdapterEvent::MessageUpdated {
                    message: self.message(turn, message_id, &segment.text, streaming),
                },
                AdapterEvent::TurnItemUpdated {
                    item: self.item(
                        turn,
                        segment.item,
                        status,
                        TurnItemKind::AssistantMessage {
                            message_id,
                            text: segment.text.clone(),
                            streaming,
                        },
                    ),
                },
            ],
            None => vec![AdapterEvent::TurnItemUpdated {
                item: self.item(
                    turn,
                    segment.item,
                    status,
                    TurnItemKind::Reasoning {
                        text: segment.text.clone(),
                        streaming,
                    },
                ),
            }],
        }
    }

    /// Completes the open text and reasoning segments (a block boundary).
    fn close_segments(&self, turn: &mut OpenTurn) -> Vec<AdapterEvent> {
        let mut events = Vec::new();
        for segment in [turn.text.take(), turn.reasoning.take()]
            .into_iter()
            .flatten()
        {
            events.extend(self.segment_events(turn, &segment, true));
        }
        events
    }

    fn next_item_ids(&self, turn: &mut OpenTurn) -> (TurnItemId, MessageId) {
        let index = turn.next_item;
        turn.next_item += 1;
        let ordinal = (u64::from(turn.reference.ordinal) << 20) | index;
        let scope = turn.provider_thread_id.0;
        (
            self.bind_ordinal(scope, EntityKind::Item, ordinal),
            self.bind_ordinal(scope, EntityKind::Message, ordinal),
        )
    }

    /// Ends the open turn with `outcome`. Exactly one caller wins: the turn
    /// is taken out of the state first.
    async fn finish(&self, rpc_id: Option<u64>, outcome: TurnOutcome) {
        let events = {
            let mut state = self.state.lock();
            let matches = state
                .turn
                .as_ref()
                .is_some_and(|turn| rpc_id.is_none_or(|id| turn.rpc_id == id));
            if !matches {
                return;
            }
            let mut turn = state.turn.take().expect("checked above");
            let mut events = state.coalescer.flush();
            if outcome == TurnOutcome::Completed {
                events.extend(self.close_segments(&mut turn));
            }
            let status = match &outcome {
                TurnOutcome::Completed => ProviderTurnStatus::Completed,
                TurnOutcome::Interrupted => ProviderTurnStatus::Interrupted,
                TurnOutcome::Cancelled => ProviderTurnStatus::Cancelled,
                TurnOutcome::Failed { .. } => ProviderTurnStatus::Failed,
            };
            // Open tools did not finish.
            if outcome != TurnOutcome::Completed {
                let mut open = turn.tools.drain().collect::<Vec<_>>();
                open.sort_by(|a, b| a.0.cmp(&b.0));
                for (native, tool) in open {
                    events.extend(self.tool_events(
                        &turn,
                        &native,
                        &tool,
                        ItemStatus::Interrupted,
                        &Value::Null,
                    ));
                }
            }
            // Requests the turn left open can no longer be answered.
            let now = self.now();
            let mut stale = state
                .requests
                .drain()
                .map(|(_, pending)| pending.request)
                .collect::<Vec<_>>();
            stale.sort_by_key(|request| request.id);
            for request in stale {
                events.push(AdapterEvent::RuntimeRequestUpdated {
                    request: RuntimeRequest {
                        status: RequestStatus::Cancelled,
                        resolved_at: Some(now),
                        ..request
                    },
                });
            }
            events.push(AdapterEvent::ProviderTurnUpdated {
                turn: self.turn_snapshot(&turn, status),
            });
            events.push(AdapterEvent::TurnTerminal {
                terminal: TurnTerminal {
                    run: turn.run,
                    provider_turn: Some(turn.reference.clone()),
                    outcome,
                    thread_disposition: ThreadDisposition::Reusable,
                    child_turns_continue: false,
                    background_pending: false,
                    summary: None,
                },
            });
            events
        };
        self.emit(events).await;
    }

    /// The `session/prompt` response.
    async fn prompt_settled(self: Arc<Self>, rpc_id: u64, result: Result<Value, RpcError>) {
        let (interrupted, produced, kimi) = {
            let state = self.state.lock();
            let Some(turn) = state.turn.as_ref().filter(|turn| turn.rpc_id == rpc_id) else {
                return;
            };
            (
                turn.interrupted,
                turn.produced_content,
                turn.kimi_offset
                    .map(|offset| (turn.native_session.clone(), offset)),
            )
        };
        let outcome = match result {
            Ok(response) => {
                let stop = response
                    .get("stopReason")
                    .and_then(Value::as_str)
                    .unwrap_or("end_turn");
                let mut outcome = stop_outcome(stop, interrupted);
                // Kimi ends a failed turn with `end_turn` and no content; its
                // wire log is the only place that names the cause.
                if outcome == TurnOutcome::Completed
                    && !produced
                    && let Some((session, offset)) = kimi
                {
                    let home = self.setup.homes.kimi.clone();
                    let failure = tokio::task::spawn_blocking(move || {
                        crate::kimi_session::turn_failure(&home, &session, offset)
                    })
                    .await
                    .ok()
                    .flatten();
                    if let Some(failure) = failure {
                        outcome = TurnOutcome::Failed {
                            failure: flavor::kimi_failure(
                                failure.code.as_deref(),
                                &failure.message,
                            ),
                        };
                    }
                }
                outcome
            }
            Err(error) if interrupted && !error.is_closed() => TurnOutcome::Interrupted,
            Err(error) => {
                let mut failure = flavor::classify_rpc_failure(self.flavor(), &error, self.now());
                if error.is_closed()
                    && let Some(detail) = self.stderr_error()
                {
                    failure.message = detail;
                }
                TurnOutcome::Failed { failure }
            }
        };
        self.finish(Some(rpc_id), outcome).await;
    }

    async fn start_turn(self: &Arc<Self>, req: TurnStart) -> DriverResult<TurnAccepted> {
        let _op = self.op.lock().await;
        {
            let mut state = self.state.lock();
            if state.turn.is_some() || state.starting.is_some() {
                return Err(DriverError::NotReady(NotReadyReason::PreviousTurnActive));
            }
            state.starting = Some(Starting {
                attempt: req.run.attempt_id,
                cancelled: false,
            });
            state.selection = req.selection.clone();
        }
        let result = self.send_prompt(req).await;
        self.state.lock().starting = None;
        result
    }

    async fn send_prompt(self: &Arc<Self>, req: TurnStart) -> DriverResult<TurnAccepted> {
        let reference = ProviderTurnRef {
            provider_turn_id: self.bind_ordinal(
                req.thread.provider_thread_id.0,
                EntityKind::ProviderTurn,
                u64::from(req.provider_turn_ordinal),
            ),
            native: NativeRef {
                id: String::new(),
                strength: RefStrength::None,
            },
            ordinal: req.provider_turn_ordinal,
        };
        let native = self.ensure_active(&req.thread.native.id).await?;
        // A per-turn selection change (`ApplyOnNextTurn`) reaches the agent
        // here, before the prompt.
        let choice = ModelChoice::from_selection(&req.selection);
        let pending = {
            let state = self.state.lock();
            state
                .active
                .as_ref()
                .filter(|active| active.applied != choice)
                .map(|active| active.config_options.clone())
        };
        if let Some(options) = pending {
            match selection::apply_model(
                &self.connection,
                self.flavor(),
                &native,
                &options,
                &choice,
            )
            .await
            {
                Ok(()) => {
                    if let Some(active) = self.state.lock().active.as_mut() {
                        active.applied = choice;
                    }
                }
                Err(error) => self.model_error(&error),
            }
        }
        let started_at = self.now();
        let cancelled = self
            .state
            .lock()
            .starting
            .as_ref()
            .is_some_and(|starting| starting.cancelled);
        if cancelled {
            // Interrupted before the agent saw the prompt: the turn never ran.
            self.emit(vec![AdapterEvent::TurnTerminal {
                terminal: TurnTerminal {
                    run: req.run,
                    provider_turn: Some(reference.clone()),
                    outcome: TurnOutcome::Cancelled,
                    thread_disposition: ThreadDisposition::Reusable,
                    child_turns_continue: false,
                    background_pending: false,
                    summary: None,
                },
            }])
            .await;
            return Ok(TurnAccepted {
                provider_turn: Some(reference),
            });
        }
        let text = wrap_synthetic_context(&req.app_context, &req.message.text);
        let mut params = json!({
            "sessionId": native,
            "prompt": [{ "type": "text", "text": text }],
        });
        let prompt_id = (self.flavor() == super::flavor::Flavor::Grok).then(|| {
            format!(
                "waku-prompt-{}",
                self.prompts.fetch_add(1, Ordering::Relaxed)
            )
        });
        if let Some(prompt_id) = &prompt_id {
            params["_meta"] = json!({ "promptId": prompt_id, "requestId": prompt_id });
        }
        let kimi_offset = if self.flavor() == super::flavor::Flavor::Kimi {
            let (home, session) = (self.setup.homes.kimi.clone(), native.clone());
            tokio::task::spawn_blocking(move || {
                crate::kimi_session::wire_offset_in(&home, &session)
            })
            .await
            .ok()
        } else {
            None
        };
        let turn = OpenTurn {
            run: req.run,
            reference: reference.clone(),
            provider_thread_id: req.thread.provider_thread_id,
            native_session: native.clone(),
            started_at,
            rpc_id: 0,
            prompt_id,
            interrupted: false,
            produced_content: false,
            kimi_offset,
            next_item: 0,
            text: None,
            reasoning: None,
            tools: HashMap::new(),
            usage: None,
        };
        // The running turn is reported before the prompt is written, so every
        // update the agent sends lands on a started turn.
        let running = self.turn_snapshot(&turn, ProviderTurnStatus::Running);
        self.state.lock().turn = Some(turn);
        self.emit(vec![AdapterEvent::ProviderTurnUpdated { turn: running }])
            .await;
        let (rpc_id, receiver) = match self.connection.send_request("session/prompt", params).await
        {
            Ok(sent) => sent,
            Err(error) => {
                self.state.lock().turn = None;
                return Err(error);
            }
        };
        if let Some(turn) = self.state.lock().turn.as_mut() {
            turn.rpc_id = rpc_id;
        }
        let shared = self.clone();
        tokio::spawn(async move {
            let result = receiver.await.unwrap_or_else(|_| Err(RpcError::closed()));
            shared.prompt_settled(rpc_id, result).await;
        });
        Ok(TurnAccepted {
            provider_turn: Some(reference),
        })
    }

    async fn interrupt(&self, req: &Interrupt) -> DriverResult<()> {
        let (native, pending) = {
            let mut state = self.state.lock();
            match state.turn.as_mut() {
                Some(turn) if turn.run.attempt_id == req.run.attempt_id => {
                    turn.interrupted = true;
                    let native = turn.native_session.clone();
                    let pending = state.requests.drain().map(|(_, pending)| pending).collect();
                    (native, pending)
                }
                _ => {
                    if let Some(starting) = state.starting.as_mut()
                        && starting.attempt == req.run.attempt_id
                    {
                        starting.cancelled = true;
                    }
                    return Ok(());
                }
            }
        };
        self.connection
            .notify("session/cancel", json!({ "sessionId": native }))
            .await?;
        self.cancel_requests(pending).await;
        Ok(())
    }

    async fn cancel_requests(&self, mut pending: Vec<PendingRequest>) {
        pending.sort_by_key(|pending| pending.request.id);
        let now = self.now();
        let mut events = Vec::new();
        for pending in pending {
            let reply = match &pending.kind {
                PendingKind::Permission => json!({ "outcome": { "outcome": "cancelled" } }),
                PendingKind::Question { kind, .. } => kind.cancelled(),
            };
            let _ = self.connection.respond(&pending.id, reply).await;
            events.push(AdapterEvent::RuntimeRequestUpdated {
                request: RuntimeRequest {
                    status: RequestStatus::Cancelled,
                    resolved_at: Some(now),
                    ..pending.request
                },
            });
        }
        self.emit(events).await;
    }

    // -- inbound --------------------------------------------------------------------

    pub async fn handle_frame(self: &Arc<Self>, line: &str) {
        let Ok(frame) = serde_json::from_str::<Value>(line) else {
            self.note(line);
            return;
        };
        match decode(&frame) {
            Some(Inbound::Response { id, result }) => {
                if !self.connection.resolve(id, result) {
                    self.note(line);
                }
            }
            Some(Inbound::Request { id, method, params }) => {
                self.handle_request(id, &method, params, line).await;
            }
            Some(Inbound::Notification { method, params }) => {
                self.handle_notification(&method, &params, line).await;
            }
            None => self.note(line),
        }
    }

    async fn handle_notification(&self, method: &str, params: &Value, line: &str) {
        match method {
            "session/update" => self.session_update(params, line).await,
            "_x.ai/session/prompt_complete" | "x.ai/session/prompt_complete" => {
                self.xai_prompt_complete(params).await;
            }
            _ => self.note(line),
        }
    }

    /// Grok can report a prompt's end before (or instead of) the
    /// `session/prompt` response; the first of the two settles the turn.
    async fn xai_prompt_complete(&self, params: &Value) {
        let prompt_id = params.get("promptId").and_then(Value::as_str);
        let (rpc_id, interrupted) = {
            let state = self.state.lock();
            let Some(turn) = state.turn.as_ref().filter(|turn| {
                params.get("sessionId").and_then(Value::as_str) == Some(&turn.native_session)
                    && prompt_id.is_none_or(|id| turn.prompt_id.as_deref() == Some(id))
            }) else {
                return;
            };
            (turn.rpc_id, turn.interrupted)
        };
        let stop = params
            .get("stopReason")
            .and_then(Value::as_str)
            .unwrap_or("end_turn");
        let outcome = match flavor::xai_completion_failure(
            stop,
            params.get("agentResult").and_then(Value::as_str),
        ) {
            Some(failure) if !interrupted => TurnOutcome::Failed { failure },
            Some(_) => TurnOutcome::Interrupted,
            None => stop_outcome(stop, interrupted),
        };
        self.finish(Some(rpc_id), outcome).await;
    }

    async fn session_update(&self, params: &Value, line: &str) {
        let update = params.get("update").cloned().unwrap_or(Value::Null);
        let kind = update
            .get("sessionUpdate")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        {
            let mut state = self.state.lock();
            if state.suppress_updates {
                if let Some(capture) = state.capture.as_mut() {
                    capture.push(update);
                }
                return;
            }
        }
        match kind.as_str() {
            "available_commands_update" => {
                let commands = update
                    .get("availableCommands")
                    .and_then(Value::as_array)
                    .map(|list| {
                        list.iter()
                            .filter_map(|command| {
                                Some(ReportedCommand {
                                    name: command.get("name").and_then(Value::as_str)?.to_owned(),
                                    description: command
                                        .get("description")
                                        .and_then(Value::as_str)
                                        .unwrap_or_default()
                                        .to_owned(),
                                })
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                if !commands.is_empty() {
                    let reported = commands
                        .iter()
                        .map(|command| SessionCommand {
                            name: command.name.clone(),
                            description: Some(command.description.clone())
                                .filter(|description| !description.is_empty()),
                        })
                        .collect();
                    self.emit(vec![AdapterEvent::AvailableCommandsUpdated {
                        commands: reported,
                    }])
                    .await;
                }
                return;
            }
            "session_info_update" => return,
            // Waku's own prompt echoed back.
            "user_message_chunk" => return,
            _ => {}
        }
        let events = {
            let mut guard = self.state.lock();
            let state = &mut *guard;
            let Some(turn) = state.turn.as_mut() else {
                drop(guard);
                self.note(line);
                return;
            };
            let events = match kind.as_str() {
                "agent_message_chunk" | "agent_thought_chunk" => {
                    let text = str_at(&update, "/content/text").unwrap_or_default();
                    let thought = kind == "agent_thought_chunk";
                    if text.is_empty()
                        || (!thought
                            && self.flavor() == Flavor::Fx
                            && !turn.produced_content
                            && fx_context_notice(text))
                    {
                        Vec::new()
                    } else {
                        turn.produced_content = true;
                        self.chunk(turn, text, thought)
                    }
                }
                "tool_call" | "tool_call_update" => {
                    turn.produced_content = true;
                    let mut events = self.close_segments(turn);
                    events.extend(self.tool_update(turn, &update));
                    events
                }
                "plan" => {
                    turn.produced_content = true;
                    vec![self.plan(turn, &update)]
                }
                "usage_update" => {
                    let used = update.get("used").and_then(Value::as_u64).unwrap_or(0);
                    let max = ["size", "max", "limit", "contextWindow", "context_window"]
                        .into_iter()
                        .find_map(|key| update.get(key).and_then(Value::as_u64))
                        .filter(|max| *max > 0);
                    if used == 0 && max.is_none() {
                        Vec::new()
                    } else {
                        turn.usage = Some(TokenUsage {
                            used,
                            max,
                            input: None,
                            cached_input: None,
                            output: None,
                            reasoning_output: None,
                            prompt_cache_ttl_ms: None,
                            updated_at: self.now(),
                        });
                        vec![AdapterEvent::ProviderTurnUpdated {
                            turn: self.turn_snapshot(turn, ProviderTurnStatus::Running),
                        }]
                    }
                }
                _ => {
                    drop(guard);
                    self.note(line);
                    return;
                }
            };
            events
                .into_iter()
                .flat_map(|event| state.coalescer.offer(event))
                .collect::<Vec<_>>()
        };
        self.emit(events).await;
    }

    fn chunk(&self, turn: &mut OpenTurn, text: &str, thought: bool) -> Vec<AdapterEvent> {
        let mut events = Vec::new();
        // A switch between text and reasoning closes the other block.
        let other = if thought {
            turn.text.take()
        } else {
            turn.reasoning.take()
        };
        if let Some(other) = other {
            events.extend(self.segment_events(turn, &other, true));
        }
        let open = if thought {
            turn.reasoning.take()
        } else {
            turn.text.take()
        };
        let mut segment = match open {
            Some(segment) => segment,
            None => {
                let (item, message) = self.next_item_ids(turn);
                Segment {
                    item,
                    message: (!thought).then_some(message),
                    text: String::new(),
                }
            }
        };
        segment.text.push_str(text);
        events.extend(self.segment_events(turn, &segment, false));
        if thought {
            turn.reasoning = Some(segment);
        } else {
            turn.text = Some(segment);
        }
        events
    }

    fn plan(&self, turn: &OpenTurn, update: &Value) -> AdapterEvent {
        let steps = update
            .get("entries")
            .and_then(Value::as_array)
            .map(|entries| {
                entries
                    .iter()
                    .enumerate()
                    .map(|(index, entry)| PlanStep {
                        id: format!("acp-step-{}", index + 1),
                        text: entry
                            .get("content")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                        status: match entry.get("status").and_then(Value::as_str) {
                            Some("completed") => PlanStepStatus::Completed,
                            Some("in_progress") => PlanStepStatus::Running,
                            _ => PlanStepStatus::Pending,
                        },
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let done = !steps.is_empty()
            && steps
                .iter()
                .all(|step| step.status == PlanStepStatus::Completed);
        AdapterEvent::PlanUpdated {
            plan: Plan {
                // ACP sends the whole plan each time: one plan per turn.
                id: self.bind_ordinal(
                    turn.provider_thread_id.0,
                    EntityKind::Plan,
                    u64::from(turn.reference.ordinal),
                ),
                thread_id: turn.run.thread_id,
                run_id: Some(turn.run.run_id),
                node_id: turn.run.root_node_id,
                status: if done {
                    PlanStatus::Completed
                } else {
                    PlanStatus::Active
                },
                kind: PlanKind::TodoList {
                    steps,
                    explanation: None,
                },
            },
        }
    }

    fn tool_update(&self, turn: &mut OpenTurn, update: &Value) -> Vec<AdapterEvent> {
        let Some(native) = update.get("toolCallId").and_then(Value::as_str) else {
            return Vec::new();
        };
        let status = match update.get("status").and_then(Value::as_str) {
            Some("completed") => ItemStatus::Completed,
            Some("failed") => ItemStatus::Failed,
            _ => ItemStatus::Running,
        };
        let now = self.now();
        let scope = turn.provider_thread_id.0;
        let known = turn.tools.contains_key(native);
        let mut tool = turn.tools.remove(native).unwrap_or_else(|| ToolState {
            item: self.bind(scope, EntityKind::Item, native),
            node: self.bind(scope, EntityKind::Node, native),
            kind: String::new(),
            title: String::new(),
            input: Value::Null,
            started_at: now,
        });
        if let Some(kind) = update.get("kind").and_then(Value::as_str) {
            tool.kind = kind.to_owned();
        }
        if let Some(title) = update
            .get("title")
            .and_then(Value::as_str)
            .filter(|title| !title.is_empty())
        {
            tool.title = title.to_owned();
        }
        if let Some(input) = update.get("rawInput").filter(|input| !input.is_null()) {
            tool.input = input.clone();
        }
        if !known && tool.title.is_empty() {
            tool.title = "Tool".to_owned();
        }
        let events = self.tool_events(turn, native, &tool, status, update);
        if !status.is_terminal() {
            turn.tools.insert(native.to_owned(), tool);
        }
        events
    }

    fn tool_events(
        &self,
        turn: &OpenTurn,
        native: &str,
        tool: &ToolState,
        status: ItemStatus,
        update: &Value,
    ) -> Vec<AdapterEvent> {
        let output = tool_output(update);
        let kind = tool_kind(tool, update, output.clone(), status);
        let mut row = self.item(turn, tool.item, status, kind);
        let reference = NativeRef::weak(native).to_provider_ref(self.flavor().driver_kind());
        row.node_id = Some(tool.node);
        row.native_item_ref = Some(reference.clone());
        row.title = Some(tool.title.clone());
        row.started_at = Some(tool.started_at);
        let node = ExecutionNode {
            id: tool.node,
            thread_id: turn.run.thread_id,
            run_id: Some(turn.run.run_id),
            parent_node_id: Some(turn.run.root_node_id),
            root_node_id: turn.run.root_node_id,
            kind: NodeKind::ToolCall,
            status: match status {
                ItemStatus::Completed => NodeStatus::Completed,
                ItemStatus::Failed => NodeStatus::Failed,
                ItemStatus::Interrupted => NodeStatus::Interrupted,
                _ => NodeStatus::Running,
            },
            counts_for_run: false,
            provider_thread_id: Some(turn.provider_thread_id),
            provider_turn_id: Some(turn.reference.provider_turn_id),
            native_item_ref: Some(reference),
            runtime_request_id: None,
            checkpoint_scope_id: None,
            started_at: Some(tool.started_at),
            completed_at: status.is_terminal().then(|| self.now()),
        };
        vec![
            AdapterEvent::NodeUpdated { node },
            AdapterEvent::TurnItemUpdated { item: row },
        ]
    }

    async fn handle_request(self: &Arc<Self>, id: Value, method: &str, params: Value, line: &str) {
        if method == "session/request_permission" {
            self.permission(id, params).await;
            return;
        }
        if let Some(kind) = QuestionKind::from_method(method) {
            self.question(id, kind, &params).await;
            return;
        }
        self.note(line);
        let _ = self
            .connection
            .respond_error(&id, METHOD_NOT_FOUND, "Method not found")
            .await;
    }

    fn request_entity(
        &self,
        turn: &OpenTurn,
        key: &str,
        kind: RuntimeRequestKind,
        options: Vec<PermissionOption>,
        questions: Vec<UserInputQuestion>,
    ) -> RuntimeRequest {
        // Agents restart request ids with every process, so the binding is
        // scoped by the provider session as well as the thread.
        let native = format!("{}:{key}", self.setup.provider_session_id);
        RuntimeRequest {
            id: self.bind(turn.provider_thread_id.0, EntityKind::Request, &native),
            thread_id: turn.run.thread_id,
            node_id: turn.run.root_node_id,
            provider_turn_id: Some(turn.reference.provider_turn_id),
            native_request_ref: Some(ProviderRef::strong(self.flavor().driver_kind(), key)),
            kind,
            status: RequestStatus::Pending,
            response_capability: ResponseCapability::Live {
                provider_session_id: self.setup.provider_session_id,
            },
            options,
            questions,
            response: None,
            created_at: self.now(),
            resolved_at: None,
        }
    }

    async fn permission(&self, id: Value, params: Value) {
        let options = params
            .get("options")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let option_kind = |option: &Value| {
            option
                .get("kind")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        let auto_approve = self.state.lock().selection.runtime_mode != RuntimeMode::Ask;
        let has_turn = self.state.lock().turn.is_some();
        if auto_approve || !has_turn {
            // Outside Supervised mode the agent's own policy has already
            // decided to ask; Waku grants the broadest allow it offers.
            let choice = auto_approve
                .then(|| {
                    ["allow_always", "allow_once"].iter().find_map(|wanted| {
                        options
                            .iter()
                            .find(|option| option_kind(option) == *wanted)
                            .and_then(|option| option.get("optionId").cloned())
                    })
                })
                .flatten();
            let outcome = match choice {
                Some(option_id) => {
                    json!({ "outcome": { "outcome": "selected", "optionId": option_id } })
                }
                None => json!({ "outcome": { "outcome": "cancelled" } }),
            };
            let _ = self.connection.respond(&id, outcome).await;
            return;
        }
        let key = native_key(&id);
        let tool_kind = str_at(&params, "/toolCall/kind").unwrap_or_default();
        let kind = match tool_kind {
            "execute" => RuntimeRequestKind::CommandApproval,
            "edit" | "delete" | "move" => RuntimeRequestKind::FileChangeApproval,
            "read" => RuntimeRequestKind::FileReadApproval,
            _ => RuntimeRequestKind::Permission,
        };
        let permission_options = options
            .iter()
            .map(|option| PermissionOption {
                id: option.get("optionId").map(native_key).unwrap_or_default(),
                label: option
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                allow: option_kind(option).starts_with("allow"),
            })
            .collect::<Vec<_>>();
        let title = str_at(&params, "/toolCall/title")
            .filter(|title| !title.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| tr!("permission.run_a_tool"));
        let prompt = match permission_reason(&params) {
            Some(reason) => format!("{title}\n{reason}"),
            None => title,
        };
        let events = {
            let mut guard = self.state.lock();
            let state = &mut *guard;
            let Some(turn) = state.turn.as_ref() else {
                return;
            };
            let request =
                self.request_entity(turn, &key, kind, permission_options.clone(), Vec::new());
            let item = self.item(
                turn,
                waku_orchestrator::provider::derived::request_item(request.id),
                ItemStatus::Waiting,
                TurnItemKind::ApprovalRequest {
                    request_id: request.id,
                    request_kind: kind,
                    prompt: Some(prompt),
                    app_name: None,
                    options: permission_options,
                },
            );
            state.requests.insert(
                key,
                PendingRequest {
                    id,
                    kind: PendingKind::Permission,
                    request: request.clone(),
                },
            );
            [
                AdapterEvent::TurnItemUpdated { item },
                AdapterEvent::RuntimeRequestUpdated { request },
            ]
            .into_iter()
            .flat_map(|event| state.coalescer.offer(event))
            .collect::<Vec<_>>()
        };
        self.emit(events).await;
    }

    async fn question(&self, id: Value, kind: QuestionKind, params: &Value) {
        let params = kind.params(params);
        let questions = kind.questions(&params);
        let key = native_key(&id);
        let events = {
            let mut guard = self.state.lock();
            let state = &mut *guard;
            match state.turn.as_ref() {
                Some(turn) if !questions.is_empty() => {
                    let request = self.request_entity(
                        turn,
                        &key,
                        RuntimeRequestKind::UserInput,
                        Vec::new(),
                        questions,
                    );
                    state.requests.insert(
                        key,
                        PendingRequest {
                            id: id.clone(),
                            kind: PendingKind::Question {
                                kind,
                                params: params.clone(),
                            },
                            request: request.clone(),
                        },
                    );
                    Some(
                        state
                            .coalescer
                            .offer(AdapterEvent::RuntimeRequestUpdated { request }),
                    )
                }
                _ => None,
            }
        };
        match events {
            Some(events) => self.emit(events).await,
            None => {
                let _ = self.connection.respond(&id, kind.cancelled()).await;
            }
        }
    }

    async fn respond(&self, req: RuntimeResponse) -> DriverResult<()> {
        let Some(pending) = self.state.lock().requests.remove(&req.native.id) else {
            return Err(DriverError::NotReady(NotReadyReason::NoActiveTurn));
        };
        let (reply, status) = match (&pending.kind, &req.reply) {
            (PendingKind::Permission, RuntimeReply::Decision { option_id }) => (
                json!({ "outcome": { "outcome": "selected", "optionId": option_id } }),
                RequestStatus::Resolved,
            ),
            (PendingKind::Question { kind, params }, RuntimeReply::Answers(answers)) => {
                (kind.response(params, answers), RequestStatus::Resolved)
            }
            (PendingKind::Permission, _) => (
                json!({ "outcome": { "outcome": "cancelled" } }),
                RequestStatus::Cancelled,
            ),
            (PendingKind::Question { kind, .. }, _) => (kind.cancelled(), RequestStatus::Cancelled),
        };
        self.connection.respond(&pending.id, reply).await?;
        let now = self.now();
        self.emit(vec![AdapterEvent::RuntimeRequestUpdated {
            request: RuntimeRequest {
                status,
                resolved_at: Some(now),
                ..pending.request
            },
        }])
        .await;
        Ok(())
    }

    /// The agent exited: every open turn gets its one terminal, then the
    /// event stream ends.
    pub async fn exited(&self, status: std::io::Result<ProcessExit>) {
        let open = self.state.lock().turn.as_ref().map(|turn| turn.rpc_id);
        if let Some(rpc_id) = open {
            let detail = self.stderr_error().unwrap_or_else(|| match &status {
                Ok(exit) if exit.success => "The agent exited.".to_owned(),
                Ok(exit) => format!("The agent exited ({:?}).", exit.code),
                Err(error) => format!("The agent failed: {error}"),
            });
            self.finish(
                Some(rpc_id),
                TurnOutcome::Failed {
                    failure: ProviderFailure::new(FailureClass::TransportError, detail)
                        .with_code("process_exit"),
                },
            )
            .await;
        }
        self.connection.fail_pending();
        self.state.lock().starting = None;
        self.events.lock().take();
    }

    async fn close(&self) {
        self.connection.close().await;
    }
}

fn stop_outcome(stop: &str, interrupted: bool) -> TurnOutcome {
    let failed = |code: &str, message: String| TurnOutcome::Failed {
        failure: ProviderFailure::new(FailureClass::ProviderError, message).with_code(code),
    };
    match stop {
        "cancelled" if interrupted => TurnOutcome::Interrupted,
        "cancelled" => TurnOutcome::Cancelled,
        "max_tokens" => failed("max_tokens", tr!("session.agent_ran_out_of_context")),
        "refusal" => failed("refusal", tr!("session.agent_declined_turn")),
        "max_turn_requests" => failed(
            "max_turn_requests",
            tr!("session.agent_stopped_reason", reason = "max_turn_requests"),
        ),
        // `end_turn`, and stop reasons newer than this driver.
        _ => TurnOutcome::Completed,
    }
}

fn fx_context_notice(text: &str) -> bool {
    text.starts_with("[context] ") || text.starts_with("skill discovery warning: ")
}

/// The text of a tool update's `content` (falling back to `rawOutput`).
fn tool_output(update: &Value) -> Option<String> {
    let mut parts = Vec::new();
    for entry in update
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(text) = entry
            .pointer("/content/text")
            .or_else(|| entry.get("text"))
            .and_then(Value::as_str)
        {
            parts.push(text.to_owned());
        }
    }
    if parts.is_empty() {
        return update
            .get("rawOutput")
            .filter(|output| !output.is_null())
            .map(|output| match output {
                Value::String(text) => text.clone(),
                other => other
                    .get("output")
                    .or_else(|| other.get("stdout"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| other.to_string()),
            });
    }
    Some(parts.join("\n"))
}

fn input_string(input: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| input.get(*key).and_then(Value::as_str))
        .map(str::to_owned)
}

/// The turn-item variant of an ACP tool call, by its `kind`.
fn tool_kind(
    tool: &ToolState,
    update: &Value,
    output: Option<String>,
    status: ItemStatus,
) -> TurnItemKind {
    let diff = update
        .get("content")
        .and_then(Value::as_array)
        .and_then(|content| {
            content
                .iter()
                .find(|entry| entry.get("type").and_then(Value::as_str) == Some("diff"))
        });
    let location = update
        .pointer("/locations/0/path")
        .and_then(Value::as_str)
        .map(str::to_owned);
    match tool.kind.as_str() {
        "execute" => {
            let exit_code = update
                .pointer("/rawOutput/exitCode")
                .or_else(|| update.pointer("/rawOutput/exit_code"))
                .and_then(Value::as_i64)
                .map(|code| code as i32);
            TurnItemKind::CommandExecution {
                input: input_string(&tool.input, &["command", "cmd"])
                    .unwrap_or_else(|| tool.title.clone()),
                output,
                output_truncated: false,
                output_omitted: false,
                output_indicates_failure: status == ItemStatus::Failed
                    || exit_code.is_some_and(|code| code != 0),
                exit_code,
            }
        }
        "edit" | "delete" | "move" => TurnItemKind::FileChange {
            file_name: diff
                .and_then(|diff| diff.get("path").and_then(Value::as_str).map(str::to_owned))
                .or(location)
                .or_else(|| input_string(&tool.input, &["path", "file_path", "filePath"]))
                .unwrap_or_else(|| tool.title.clone()),
            additions: None,
            deletions: None,
            diff: None,
            old_text: diff
                .and_then(|diff| diff.get("oldText").and_then(Value::as_str))
                .map(str::to_owned),
            new_text: diff
                .and_then(|diff| diff.get("newText").and_then(Value::as_str))
                .map(str::to_owned),
            changes: Vec::new(),
            body_omitted: false,
        },
        "search" => TurnItemKind::FileSearch {
            pattern: input_string(&tool.input, &["pattern", "query", "glob"])
                .or_else(|| Some(tool.title.clone())),
            results: Vec::new(),
        },
        "fetch" => TurnItemKind::WebSearch {
            patterns: vec![
                input_string(&tool.input, &["url", "query"]).unwrap_or_else(|| tool.title.clone()),
            ],
            results: Vec::new(),
        },
        // `read`, `think`, `other` and MCP tools.
        _ => TurnItemKind::DynamicTool {
            tool_name: Some(if tool.kind.is_empty() || tool.kind == "other" {
                tool.title.clone()
            } else {
                tool.kind.clone()
            }),
            input: JsonText(tool.input.to_string()),
            output: output.map(|output| JsonText(Value::String(output).to_string())),
            output_omitted: false,
            viewed_image_path: None,
        },
    }
}

/// Reads the agent's stdout until it ends.
pub async fn read_loop(
    shared: Arc<Shared>,
    stdout: Box<dyn AsyncRead + Send + Unpin>,
    exit: futures_util::future::BoxFuture<'static, std::io::Result<ProcessExit>>,
) {
    let mut lines = BufReader::new(stdout).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        shared.handle_frame(&line).await;
    }
    let status = exit.await;
    shared.exited(status).await;
}

/// Keeps the last stderr lines for error messages.
pub async fn stderr_loop(shared: Arc<Shared>, stderr: Box<dyn AsyncRead + Send + Unpin>) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        shared.push_stderr(line);
    }
}

// -- the session handle -------------------------------------------------------------

pub struct AcpSession {
    pub shared: Arc<Shared>,
}

impl AcpSession {
    fn require_native_fork(&self, operation: &'static str) -> DriverResult<()> {
        if self.shared.caps().threads.fork {
            Ok(())
        } else {
            Err(DriverError::Unsupported { operation })
        }
    }

    async fn grok_fork(&self, source: &str, retained: Option<usize>) -> DriverResult<String> {
        let setup = &self.shared.setup;
        super::grok::fork_session(
            &setup.transports,
            &setup.binary,
            &setup.cwd,
            &setup.homes.grok,
            source,
            retained,
            setup.raw_log.clone(),
        )
        .await
    }
}

#[async_trait]
impl DriverSession for AcpSession {
    fn capabilities(&self) -> &Capabilities {
        self.shared.caps()
    }

    async fn ensure_thread(&self, req: EnsureThread) -> DriverResult<ProviderThreadRef> {
        let _op = self.shared.op.lock().await;
        self.shared.state.lock().selection = req.selection.clone();
        let native = self.shared.ensure_active("").await?;
        let provider_thread_id = match &req.existing {
            Some(existing) => existing.provider_thread_id,
            None => self.shared.bind(
                self.shared.setup.provider_session_id.0,
                EntityKind::ProviderThread,
                &native,
            ),
        };
        Ok(self.shared.thread_ref(provider_thread_id, &native))
    }

    async fn resume_thread(&self, req: ResumeThread) -> DriverResult<ProviderThreadRef> {
        let _op = self.shared.op.lock().await;
        self.shared.state.lock().selection = req.selection.clone();
        let native = self.shared.ensure_active(&req.thread.native.id).await?;
        Ok(self
            .shared
            .thread_ref(req.thread.provider_thread_id, &native))
    }

    async fn start_turn(&self, req: TurnStart) -> DriverResult<TurnAccepted> {
        self.shared.start_turn(req).await
    }

    async fn steer_turn(&self, _req: Steer) -> DriverResult<()> {
        // D-7: ACP has no native steering; the orchestrator interrupts and
        // restarts instead (`turns.steering_by_interrupt_restart`).
        Err(DriverError::Unsupported {
            operation: "steer_turn",
        })
    }

    async fn interrupt_turn(&self, req: Interrupt) -> DriverResult<()> {
        self.shared.interrupt(&req).await
    }

    async fn respond(&self, req: RuntimeResponse) -> DriverResult<()> {
        self.shared.respond(req).await
    }

    async fn read_snapshot(&self, thread: &ProviderThreadRef) -> DriverResult<ThreadSnapshot> {
        if !self.shared.caps().threads.read_snapshot {
            return Err(DriverError::Unsupported {
                operation: "read_snapshot",
            });
        }
        let _op = self.shared.op.lock().await;
        if self.shared.state.lock().turn.is_some() {
            return Err(DriverError::NotReady(NotReadyReason::PreviousTurnActive));
        }
        {
            let mut state = self.shared.state.lock();
            state.suppress_updates = true;
            state.capture = Some(Vec::new());
        }
        let response = self
            .shared
            .connection
            .request(
                "session/load",
                json!({
                    "sessionId": thread.native.id,
                    "cwd": self.shared.setup.cwd,
                    "mcpServers": [],
                }),
            )
            .await;
        let updates = {
            let mut state = self.shared.state.lock();
            state.suppress_updates = false;
            state.capture.take().unwrap_or_default()
        };
        let response =
            response.map_err(|error| self.shared.request_error("session/load", error))?;
        self.shared
            .activate(thread.native.id.clone(), &response)
            .await;
        Ok(snapshot_from_updates(&self.shared, thread, &updates))
    }

    async fn rollback(&self, req: Rollback) -> DriverResult<RollbackOutcome> {
        self.require_native_fork("rollback")?;
        let _op = self.shared.op.lock().await;
        if self.shared.state.lock().turn.is_some() {
            return Err(DriverError::NotReady(NotReadyReason::PreviousTurnActive));
        }
        let retained = match &req.target {
            RollbackTarget::ThreadStart => 0,
            RollbackTarget::AfterProviderTurn(turn) => turn.ordinal as usize,
        };
        let native = if retained == 0 {
            // Nothing to keep: a fresh native session.
            self.shared.new_session().await?
        } else {
            self.grok_fork(&req.thread.native.id, Some(retained))
                .await?
        };
        let provider_thread_id = self.shared.bind(
            req.thread.provider_thread_id.0,
            EntityKind::ProviderThread,
            &native,
        );
        Ok(RollbackOutcome {
            thread: self.shared.thread_ref(provider_thread_id, &native),
            replaced: true,
            snapshot: None,
            retained_native_turn_ids: None,
        })
    }

    async fn fork(&self, req: ForkRequest) -> DriverResult<ProviderThreadRef> {
        self.require_native_fork("fork")?;
        let retained = match &req.at {
            ForkPoint::Tip => None,
            ForkPoint::AfterProviderTurn { turn, .. } => Some(turn.ordinal as usize),
            ForkPoint::SubagentThread(_) => {
                return Err(DriverError::Unsupported {
                    operation: "fork_from_subagent_thread",
                });
            }
        };
        let _op = self.shared.op.lock().await;
        let native = self.grok_fork(&req.source.native.id, retained).await?;
        let provider_thread_id = self.shared.bind(
            req.source.provider_thread_id.0,
            EntityKind::ProviderThread,
            &native,
        );
        Ok(self.shared.thread_ref(provider_thread_id, &native))
    }

    async fn apply_selection(&self, to: &Selection) -> DriverResult<()> {
        let _op = self.shared.op.lock().await;
        let active = {
            let mut state = self.shared.state.lock();
            state.selection = to.clone();
            state
                .active
                .as_ref()
                .map(|active| (active.id.clone(), active.config_options.clone()))
        };
        // Without a session the choice applies when one is created.
        let Some((native, options)) = active else {
            return Ok(());
        };
        let choice = ModelChoice::from_selection(to);
        selection::apply_model(
            &self.shared.connection,
            self.shared.flavor(),
            &native,
            &options,
            &choice,
        )
        .await
        .map_err(|error| {
            DriverError::Provider(ProviderFailure::new(FailureClass::ValidationError, error))
        })?;
        if let Some(active) = self.shared.state.lock().active.as_mut() {
            active.applied = choice;
        }
        Ok(())
    }

    async fn close(&self) -> DriverResult<()> {
        self.shared.close().await;
        Ok(())
    }
}

/// The user-visible conversation a `session/load` replayed.
fn snapshot_from_updates(
    shared: &Shared,
    thread: &ProviderThreadRef,
    updates: &[Value],
) -> ThreadSnapshot {
    let now = shared.now();
    let setup = &shared.setup;
    let mut messages: Vec<Message> = Vec::new();
    let mut agent_since_user = true;
    for update in updates {
        let kind = update.get("sessionUpdate").and_then(Value::as_str);
        let role = match kind {
            Some("user_message_chunk") => MessageRole::User,
            Some("agent_message_chunk") => MessageRole::Assistant,
            Some("agent_thought_chunk" | "tool_call" | "tool_call_update" | "plan") => {
                agent_since_user = true;
                continue;
            }
            _ => continue,
        };
        let Some(text) = str_at(update, "/content/text").filter(|text| !text.is_empty()) else {
            continue;
        };
        if role == MessageRole::Assistant && setup.flavor == Flavor::Fx && fx_context_notice(text) {
            continue;
        }
        let append = messages.last_mut().filter(|last| {
            last.role == role && (role == MessageRole::Assistant || !agent_since_user)
        });
        match append {
            Some(last) => last.text.push_str(text),
            None => {
                let index = messages.len() as u64;
                messages.push(Message {
                    id: shared.bind_ordinal(
                        thread.provider_thread_id.0,
                        EntityKind::Message,
                        index,
                    ),
                    thread_id: setup.thread_id,
                    run_id: None,
                    node_id: None,
                    role,
                    created_by: if role == MessageRole::User {
                        Actor::User
                    } else {
                        Actor::Agent
                    },
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
                });
            }
        }
        agent_since_user = role != MessageRole::User;
    }
    let mut row = ProviderThread {
        id: thread.provider_thread_id,
        driver: setup.flavor.driver_kind(),
        provider_instance_id: setup.instance_id.clone(),
        provider_session_id: Some(setup.provider_session_id),
        app_thread_id: Some(setup.thread_id),
        owner_node_id: None,
        native_thread_ref: None,
        native_conversation_head_ref: None,
        status: ProviderThreadStatus::Idle,
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
    };
    thread.apply_to(&mut row);
    ThreadSnapshot {
        thread: row,
        turns: Vec::new(),
        messages,
        pending_requests: Vec::new(),
    }
}

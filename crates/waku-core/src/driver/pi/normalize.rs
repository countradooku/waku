//! Pi RPC events → normalized `AdapterEvent`s (RFC 0002 "Pi and Oh My Pi",
//! the `DriverEvent` → `AdapterEvent` table).
//!
//! [`Normalizer`] is the session's stream state: the open turn, its
//! streaming messages and tool calls, and the dialogs waiting for an answer.
//! It is pure — frames in, events and writes out — so every path is unit
//! tested without a process.
//!
//! Identity: Pi's events carry no turn or message ids. Turns bind by
//! ordinal (`RefStrength::None`); assistant messages by a key derived from
//! the turn ordinal and the message's position in the turn (weak); tool
//! calls by Pi's `toolCallId`; dialogs by the `extension_ui_request` id
//! (a UUID Pi mints per dialog, strong).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use serde_json::{Value, json};
use waku_orchestrator::provider::derived;
use waku_protocol::orchestration::*;

use super::protocol::{self, ALLOW, DENY, OMP_APPROVE, OMP_DENY, PiFlavor};
use crate::driver::v2::coalescer::DeltaCoalescer;
use crate::driver::v2::{
    AdapterEvent, Clock, EntityKind, IdAllocator, NativeRef, ProviderThreadRef, ProviderTurnRef,
    RunRef, RuntimeReply, ThreadDisposition, TurnOutcome, TurnTerminal,
};

/// What a normalized frame asks the session to do.
#[derive(Default, Debug)]
pub struct Output {
    pub events: Vec<AdapterEvent>,
    /// Frames to write back (dialogs that cannot be shown).
    pub writes: Vec<Value>,
    /// The open turn was accepted (`Ok`) or refused before it started.
    pub accepted: Option<Result<(), String>>,
    /// A response to a request other than the turn's prompt.
    pub response: Option<(String, Result<Value, String>)>,
    /// The native title changed.
    pub title: Option<Option<String>>,
    /// The open turn ended (its terminal is in `events`).
    pub turn_ended: bool,
}

/// Fixed per session.
pub struct Scope {
    pub flavor: PiFlavor,
    pub session_id: ProviderSessionId,
    pub ids: Arc<dyn IdAllocator>,
    pub clock: Arc<dyn Clock>,
}

pub struct OpenTurn {
    pub run: RunRef,
    pub reference: ProviderTurnRef,
    pub provider_thread_id: ProviderThreadId,
    pub started_at: Timestamp,
    /// The id of the `prompt` request that opened the turn.
    pub prompt_id: String,
    /// `start_turn` resolved: the turn owes exactly one terminal.
    pub accepted: bool,
    /// Agent activity was seen (the run really started).
    pub active: bool,
    pub interrupt_requested: bool,
    aborted: bool,
    failure: Option<ProviderFailure>,
    usage_limited: bool,
    reset_at: Option<Timestamp>,
    tokens: Option<u64>,
    messages: u32,
}

#[derive(Default)]
struct MessageState {
    key: String,
    text: String,
    reasoning: String,
    text_streamed: bool,
    reasoning_streamed: bool,
    done: bool,
}

struct ToolState {
    name: String,
    input: Value,
    item_id: TurnItemId,
    node_id: NodeId,
    done: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DialogKind {
    /// Waku's extension's `confirm` (Pi).
    Confirm,
    /// Oh My Pi's own `select` between `Approve` and `Deny`.
    Approval,
    /// `select`, `input` or `editor` from an extension.
    Question,
}

struct PendingDialog {
    request: RuntimeRequest,
    kind: DialogKind,
}

pub struct Normalizer {
    scope: Scope,
    /// The provider thread the session currently serves.
    pub thread: Option<ProviderThreadRef>,
    pub turn: Option<OpenTurn>,
    /// The model's context window (`get_state`, `set_model`).
    pub window: Option<u64>,
    seq: u64,
    messages: BTreeMap<u64, MessageState>,
    current_message: Option<u64>,
    command_output: Option<u64>,
    tools: BTreeMap<u64, ToolState>,
    tool_index: HashMap<String, u64>,
    dialogs: HashMap<String, PendingDialog>,
    coalescer: DeltaCoalescer,
}

fn str_of<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
}

impl Normalizer {
    pub fn new(scope: Scope) -> Self {
        let coalescer = DeltaCoalescer::new(scope.clock.clone());
        Self {
            scope,
            thread: None,
            turn: None,
            window: None,
            seq: 0,
            messages: BTreeMap::new(),
            current_message: None,
            command_output: None,
            tools: BTreeMap::new(),
            tool_index: HashMap::new(),
            dialogs: HashMap::new(),
            coalescer,
        }
    }

    fn driver(&self) -> DriverKind {
        self.scope.flavor.driver_kind()
    }

    fn now(&self) -> Timestamp {
        self.scope.clock.now()
    }

    fn bind<T: From<uuid::Uuid>>(&self, scope: uuid::Uuid, kind: EntityKind, native: &str) -> T {
        T::from(self.scope.ids.bind(scope, kind, native))
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    fn offer(&mut self, events: Vec<AdapterEvent>) -> Vec<AdapterEvent> {
        events
            .into_iter()
            .flat_map(|event| self.coalescer.offer(event))
            .collect()
    }

    /// Registers the turn `start_turn` is about to write. Pi's events carry
    /// no turn id, so the turn binds by its ordinal.
    pub fn begin_turn(
        &mut self,
        run: RunRef,
        thread: &ProviderThreadRef,
        ordinal: u32,
        prompt_id: String,
    ) -> ProviderTurnRef {
        let provider_thread_id = thread.provider_thread_id;
        let reference = ProviderTurnRef {
            provider_turn_id: ProviderTurnId(self.scope.ids.bind_ordinal(
                provider_thread_id.0,
                EntityKind::ProviderTurn,
                u64::from(ordinal),
            )),
            native: NativeRef {
                id: String::new(),
                strength: RefStrength::None,
            },
            ordinal,
        };
        self.turn = Some(OpenTurn {
            run,
            reference: reference.clone(),
            provider_thread_id,
            started_at: self.now(),
            prompt_id,
            accepted: false,
            active: false,
            interrupt_requested: false,
            aborted: false,
            failure: None,
            usage_limited: false,
            reset_at: None,
            tokens: None,
            messages: 0,
        });
        reference
    }

    /// Drops a turn the agent never accepted (no terminal: only an accepted
    /// turn gets one).
    pub fn abandon_turn(&mut self, attempt: RunAttemptId) {
        if self
            .turn
            .as_ref()
            .is_some_and(|turn| turn.run.attempt_id == attempt && !turn.accepted)
        {
            self.turn = None;
        }
    }

    pub fn mark_interrupt(&mut self, attempt: RunAttemptId) -> bool {
        match self.turn.as_mut() {
            Some(turn) if turn.run.attempt_id == attempt => {
                turn.interrupt_requested = true;
                true
            }
            _ => false,
        }
    }

    /// The open turn of `attempt` saw no agent activity yet.
    pub fn idle_turn(&self, attempt: RunAttemptId) -> bool {
        self.turn
            .as_ref()
            .is_some_and(|turn| turn.run.attempt_id == attempt && !turn.active)
    }

    pub fn turn_open(&self, attempt: RunAttemptId) -> bool {
        self.turn
            .as_ref()
            .is_some_and(|turn| turn.run.attempt_id == attempt)
    }

    fn turn_snapshot(&self, turn: &OpenTurn, status: ProviderTurnStatus) -> ProviderTurn {
        let terminal = !matches!(
            status,
            ProviderTurnStatus::Running | ProviderTurnStatus::Pending
        );
        let now = self.now();
        ProviderTurn {
            id: turn.reference.provider_turn_id,
            provider_thread_id: turn.provider_thread_id,
            node_id: turn.run.root_node_id,
            run_attempt_id: Some(turn.run.attempt_id),
            native_turn_ref: None,
            ordinal: turn.reference.ordinal,
            status,
            started_at: Some(turn.started_at),
            completed_at: terminal.then_some(now),
            token_usage: turn.tokens.map(|used| TokenUsage {
                used,
                max: self.window,
                input: None,
                cached_input: None,
                output: None,
                reasoning_output: None,
                prompt_cache_ttl_ms: None,
                updated_at: now,
            }),
            turn_token_usage: None,
        }
    }

    /// Accepts the open turn (once) and reports it running.
    fn accept(&mut self, out: &mut Output) {
        let Some(turn) = self.turn.as_mut() else {
            return;
        };
        if turn.accepted {
            return;
        }
        turn.accepted = true;
        out.accepted = Some(Ok(()));
        let snapshot = self
            .turn
            .as_ref()
            .map(|turn| self.turn_snapshot(turn, ProviderTurnStatus::Running));
        if let Some(turn) = snapshot {
            let events = self.offer(vec![AdapterEvent::ProviderTurnUpdated { turn }]);
            out.events.extend(events);
        }
    }

    /// Agent work: the turn is accepted and active.
    fn activity(&mut self, out: &mut Output) -> bool {
        if self.turn.is_none() {
            return false;
        }
        self.accept(out);
        if let Some(turn) = self.turn.as_mut() {
            turn.active = true;
        }
        true
    }

    fn item(&self, id: TurnItemId, status: ItemStatus, kind: TurnItemKind) -> Option<TurnItem> {
        let turn = self.turn.as_ref()?;
        let now = self.now();
        Some(TurnItem {
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
        })
    }

    /// Handles one inbound frame. `None` for a frame Waku does not use, which
    /// the session sends to the raw log.
    pub fn handle(&mut self, frame: &Value) -> Option<Output> {
        let mut out = Output::default();
        let kind = frame.get("type").and_then(Value::as_str)?;
        let flavor = self.scope.flavor;
        if kind == flavor.settled_event() {
            // Oh My Pi reuses `agent_end` for intermediate settles, flagging
            // the real one with `isTerminal`.
            if frame.get("isTerminal").and_then(Value::as_bool) != Some(false) {
                self.settle(&mut out);
            }
            return Some(out);
        }
        if kind == flavor.session_info_event() {
            out.title = Some(
                str_of(frame, flavor.session_info_title_field())
                    .map(str::trim)
                    .filter(|title| !title.is_empty())
                    .map(str::to_owned),
            );
            return Some(out);
        }
        match kind {
            "response" => self.response(frame, &mut out)?,
            "agent_start" | "turn_start" => {
                self.activity(&mut out);
            }
            "message_start" => {
                if frame.pointer("/message/role").and_then(Value::as_str) == Some("assistant")
                    && self.activity(&mut out)
                {
                    self.open_message(&mut out);
                }
            }
            "message_update" => self.message_update(frame, &mut out),
            "message_end" => self.message_end(frame, &mut out),
            "tool_execution_start" | "tool_execution_update" | "tool_execution_end" => {
                self.tool(kind, frame, &mut out);
            }
            "command_output" => self.command_output(frame, &mut out),
            "auto_retry_start" => {
                let now = self.now();
                if let Some(turn) = self.turn.as_mut()
                    && protocol::is_usage_limit(frame)
                {
                    turn.usage_limited = true;
                    turn.reset_at = frame
                        .get("delayMs")
                        .and_then(Value::as_i64)
                        .map(|delay| now + delay.max(0));
                }
            }
            "auto_retry_end" => {
                if let Some(turn) = self.turn.as_mut() {
                    if frame.get("success").and_then(Value::as_bool) == Some(true) {
                        turn.failure = None;
                    } else {
                        turn.failure = Some(protocol::failure(
                            flavor,
                            str_of(frame, "finalError"),
                            turn.usage_limited,
                            turn.reset_at,
                        ));
                    }
                }
            }
            "extension_ui_request" => self.dialog(frame, &mut out),
            // Known and carrying nothing a thread shows.
            "turn_end"
            | "agent_end"
            | "agent_settled"
            | "queue_update"
            | "prompt_result"
            | "available_commands_update"
            | "config_update"
            | "compaction_start"
            | "compaction_end"
            | "extension_error" => return None,
            _ => return None,
        }
        Some(out)
    }

    fn response(&mut self, frame: &Value, out: &mut Output) -> Option<()> {
        let id = str_of(frame, "id")?.to_owned();
        let success = frame.get("success").and_then(Value::as_bool) == Some(true);
        let error = str_of(frame, "error")
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{} RPC command failed", self.scope.flavor.display_name()));
        let prompt = self.turn.as_ref().is_some_and(|turn| turn.prompt_id == id);
        if !prompt {
            out.response = Some((
                id,
                if success {
                    Ok(frame.clone())
                } else {
                    Err(error)
                },
            ));
            return Some(());
        }
        if !success {
            let accepted = self.turn.as_ref().is_some_and(|turn| turn.accepted);
            if accepted {
                let failure = ProviderFailure::new(FailureClass::Unknown, error.clone())
                    .with_code("prompt_rejected");
                self.finish_turn(TurnOutcome::Failed { failure }, Some(error), out);
            } else {
                // Refused before it started: `start_turn` fails, no terminal.
                self.turn = None;
                out.accepted = Some(Err(error));
            }
            return Some(());
        }
        self.accept(out);
        // A command an extension or built-in handled locally completes with
        // its response; no agent run follows.
        if frame.pointer("/data/agentInvoked").and_then(Value::as_bool) == Some(false) {
            self.finish_turn(TurnOutcome::Completed, None, out);
        }
        Some(())
    }

    // -- streaming -------------------------------------------------------------

    fn open_message(&mut self, out: &mut Output) {
        if let Some(previous) = self.current_message.take() {
            self.complete_message(previous, ItemStatus::Completed, out);
        }
        let Some(turn) = self.turn.as_mut() else {
            return;
        };
        turn.messages += 1;
        let key = format!("turn-{}:message-{}", turn.reference.ordinal, turn.messages);
        let seq = self.next_seq();
        self.messages.insert(
            seq,
            MessageState {
                key,
                ..MessageState::default()
            },
        );
        self.current_message = Some(seq);
    }

    fn current(&mut self, out: &mut Output) -> Option<u64> {
        if self.current_message.is_none() {
            self.open_message(out);
        }
        self.current_message
    }

    fn message_events(&self, seq: u64, status: ItemStatus) -> Vec<AdapterEvent> {
        let done = status.is_terminal();
        let (Some(state), Some(turn)) = (self.messages.get(&seq), self.turn.as_ref()) else {
            return Vec::new();
        };
        let driver = self.driver();
        let pt = turn.provider_thread_id.0;
        let mut events = Vec::new();
        if !state.reasoning.is_empty() {
            let id: TurnItemId =
                self.bind(pt, EntityKind::Item, &format!("{}#thinking", state.key));
            if let Some(item) = self.item(
                id,
                status,
                TurnItemKind::Reasoning {
                    text: state.reasoning.clone(),
                    streaming: !done,
                },
            ) {
                events.push(AdapterEvent::TurnItemUpdated { item });
            }
        }
        if !state.text.is_empty() {
            let id: TurnItemId = self.bind(pt, EntityKind::Item, &state.key);
            let message_id: MessageId = self.bind(pt, EntityKind::Message, &state.key);
            let now = self.now();
            events.push(AdapterEvent::MessageUpdated {
                message: Message {
                    id: message_id,
                    thread_id: turn.run.thread_id,
                    run_id: Some(turn.run.run_id),
                    node_id: Some(turn.run.root_node_id),
                    role: MessageRole::Assistant,
                    created_by: Actor::Agent,
                    creation_source: CreationSource::Provider,
                    text: state.text.clone(),
                    display_text: None,
                    attachments: Vec::new(),
                    context: Vec::new(),
                    notification: None,
                    sender_thread_id: None,
                    streaming: !done,
                    created_at: turn.started_at,
                    updated_at: now,
                },
            });
            if let Some(mut item) = self.item(
                id,
                status,
                TurnItemKind::AssistantMessage {
                    message_id,
                    text: state.text.clone(),
                    streaming: !done,
                },
            ) {
                item.native_item_ref = Some(NativeRef::weak(&state.key).to_provider_ref(driver));
                events.push(AdapterEvent::TurnItemUpdated { item });
            }
        }
        events
    }

    fn complete_message(&mut self, seq: u64, status: ItemStatus, out: &mut Output) {
        let Some(state) = self.messages.get_mut(&seq) else {
            return;
        };
        if state.done {
            return;
        }
        state.done = true;
        let events = self.message_events(seq, status);
        let events = self.offer(events);
        out.events.extend(events);
    }

    fn message_update(&mut self, frame: &Value, out: &mut Output) {
        let Some(update) = frame.get("assistantMessageEvent") else {
            return;
        };
        let (reasoning, delta) = match update.get("type").and_then(Value::as_str) {
            Some("text_delta") => (false, str_of(update, "delta")),
            Some("thinking_delta") => (true, str_of(update, "delta")),
            _ => return,
        };
        let Some(delta) = delta else { return };
        if !self.activity(out) {
            return;
        }
        let Some(seq) = self.current(out) else { return };
        let Some(state) = self.messages.get_mut(&seq) else {
            return;
        };
        if reasoning {
            state.reasoning.push_str(delta);
            state.reasoning_streamed = true;
        } else {
            state.text.push_str(delta);
            state.text_streamed = true;
        }
        let events = self.message_events(seq, ItemStatus::Running);
        let events = self.offer(events);
        out.events.extend(events);
    }

    fn message_end(&mut self, frame: &Value, out: &mut Output) {
        let Some(message) = frame.get("message") else {
            return;
        };
        if message.get("role").and_then(Value::as_str) != Some("assistant") || self.turn.is_none() {
            return;
        }
        self.activity(out);
        let flavor = self.scope.flavor;
        // Fill in what was not streamed (a provider without deltas).
        if let Some(seq) = self.current(out)
            && let Some(state) = self.messages.get_mut(&seq)
        {
            for block in message
                .get("content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                match block.get("type").and_then(Value::as_str) {
                    Some("text") if !state.text_streamed => {
                        if let Some(text) = str_of(block, "text") {
                            if !state.text.is_empty() {
                                state.text.push_str("\n\n");
                            }
                            state.text.push_str(text);
                        }
                    }
                    Some("thinking") if !state.reasoning_streamed => {
                        if let Some(text) = str_of(block, "thinking") {
                            state.reasoning.push_str(text);
                        }
                    }
                    _ => {}
                }
            }
        }
        let interrupted = self
            .turn
            .as_ref()
            .is_some_and(|turn| turn.interrupt_requested);
        let status = match message.get("stopReason").and_then(Value::as_str) {
            Some("aborted") => ItemStatus::Interrupted,
            Some("error") if interrupted => ItemStatus::Interrupted,
            Some("error") => ItemStatus::Failed,
            _ => ItemStatus::Completed,
        };
        if let Some(seq) = self.current_message.take() {
            self.complete_message(seq, status, out);
        }
        // The context the next call starts from, not the billed total.
        let tokens = protocol::message_context_tokens(message);
        let changed = match (self.turn.as_mut(), tokens) {
            (Some(turn), Some(tokens)) if turn.tokens != Some(tokens) => {
                turn.tokens = Some(tokens);
                true
            }
            _ => false,
        };
        if changed && let Some(turn) = self.turn.as_ref() {
            let turn = self.turn_snapshot(turn, ProviderTurnStatus::Running);
            let events = self.offer(vec![AdapterEvent::ProviderTurnUpdated { turn }]);
            out.events.extend(events);
        }
        let Some(turn) = self.turn.as_mut() else {
            return;
        };
        match message.get("stopReason").and_then(Value::as_str) {
            Some("aborted") => turn.aborted = true,
            Some("error") => {
                turn.usage_limited |= protocol::is_usage_limit(message);
                turn.failure = Some(protocol::failure(
                    flavor,
                    str_of(message, "errorMessage"),
                    turn.usage_limited,
                    turn.reset_at,
                ));
            }
            _ => {}
        }
    }

    /// Local command output (`/context`, built-ins): complete text blocks,
    /// shown as the turn's reply.
    fn command_output(&mut self, frame: &Value, out: &mut Output) {
        let Some(text) = str_of(frame, "text") else {
            return;
        };
        if !self.activity(out) {
            return;
        }
        let seq = match self.command_output {
            Some(seq) => seq,
            None => {
                let Some(turn) = self.turn.as_ref() else {
                    return;
                };
                let key = format!("turn-{}:command-output", turn.reference.ordinal);
                let seq = self.next_seq();
                self.messages.insert(
                    seq,
                    MessageState {
                        key,
                        ..MessageState::default()
                    },
                );
                self.command_output = Some(seq);
                seq
            }
        };
        let Some(state) = self.messages.get_mut(&seq) else {
            return;
        };
        if !state.text.is_empty() {
            state.text.push_str("\n\n");
        }
        state.text.push_str(text.trim_end());
        state.text_streamed = true;
        let events = self.message_events(seq, ItemStatus::Running);
        let events = self.offer(events);
        out.events.extend(events);
    }

    // -- tools -------------------------------------------------------------------

    fn tool(&mut self, kind: &str, frame: &Value, out: &mut Output) {
        let Some(id) = str_of(frame, "toolCallId").map(str::to_owned) else {
            return;
        };
        if !self.activity(out) {
            return;
        }
        let seq = match self.tool_index.get(&id) {
            Some(seq) => *seq,
            None => {
                let Some(pt) = self.turn.as_ref().map(|turn| turn.provider_thread_id.0) else {
                    return;
                };
                let seq = self.next_seq();
                let tool = ToolState {
                    name: str_of(frame, "toolName").unwrap_or("tool").to_owned(),
                    input: frame.get("args").cloned().unwrap_or(Value::Null),
                    item_id: self.bind(pt, EntityKind::Item, &id),
                    node_id: self.bind(pt, EntityKind::Node, &id),
                    done: false,
                };
                self.tool_index.insert(id.clone(), seq);
                self.tools.insert(seq, tool);
                seq
            }
        };
        if self.tools.get(&seq).is_none_or(|tool| tool.done) {
            return;
        }
        let rows = match kind {
            "tool_execution_start" => self.tool_rows(seq, &id, ItemStatus::Running, None, false),
            "tool_execution_end" => {
                let failed = frame.get("isError").and_then(Value::as_bool) == Some(true);
                let output = protocol::tool_result_text(frame.get("result"));
                if let Some(tool) = self.tools.get_mut(&seq) {
                    tool.done = true;
                }
                let interrupted = self
                    .turn
                    .as_ref()
                    .is_some_and(|turn| turn.interrupt_requested);
                // An abort ends a running tool with an error result.
                let status = match (failed, interrupted) {
                    (true, true) => ItemStatus::Interrupted,
                    (true, false) => ItemStatus::Failed,
                    _ => ItemStatus::Completed,
                };
                self.tool_rows(seq, &id, status, output, failed)
            }
            // Partial results are not streamed (`streaming.tool_output = N`).
            _ => None,
        };
        if let Some((item, node)) = rows {
            let events = self.offer(vec![
                AdapterEvent::NodeUpdated { node },
                AdapterEvent::TurnItemUpdated { item },
            ]);
            out.events.extend(events);
        }
    }

    fn tool_rows(
        &self,
        seq: u64,
        native: &str,
        status: ItemStatus,
        output: Option<String>,
        failed: bool,
    ) -> Option<(TurnItem, ExecutionNode)> {
        let tool = self.tools.get(&seq)?;
        let turn = self.turn.as_ref()?;
        let input = &tool.input;
        let kind = match tool.name.as_str() {
            "bash" => TurnItemKind::CommandExecution {
                input: str_of(input, "command").unwrap_or_default().to_owned(),
                output,
                output_truncated: false,
                output_omitted: false,
                output_indicates_failure: failed,
                exit_code: None,
            },
            "edit" | "write" => TurnItemKind::FileChange {
                file_name: str_of(input, "path")
                    .or_else(|| str_of(input, "file_path"))
                    .unwrap_or_default()
                    .to_owned(),
                additions: None,
                deletions: None,
                diff: None,
                old_text: str_of(input, "oldText").map(str::to_owned),
                new_text: str_of(input, "newText")
                    .or_else(|| str_of(input, "content"))
                    .map(str::to_owned),
                changes: Vec::new(),
                body_omitted: false,
            },
            "grep" | "find" => TurnItemKind::FileSearch {
                pattern: str_of(input, "pattern").map(str::to_owned),
                results: Vec::new(),
            },
            name => TurnItemKind::DynamicTool {
                tool_name: Some(name.to_owned()),
                input: JsonText(input.to_string()),
                output: output.map(|output| JsonText(Value::String(output).to_string())),
                output_omitted: false,
                viewed_image_path: None,
            },
        };
        let mut item = self.item(tool.item_id, status, kind)?;
        let driver = self.driver();
        item.node_id = Some(tool.node_id);
        item.native_item_ref = Some(NativeRef::weak(native).to_provider_ref(driver));
        item.title = Some(tool.name.clone());
        let now = self.now();
        let node = ExecutionNode {
            id: tool.node_id,
            thread_id: turn.run.thread_id,
            run_id: Some(turn.run.run_id),
            parent_node_id: Some(turn.run.root_node_id),
            root_node_id: turn.run.root_node_id,
            kind: NodeKind::ToolCall,
            status: match status {
                ItemStatus::Completed => NodeStatus::Completed,
                ItemStatus::Failed => NodeStatus::Failed,
                ItemStatus::Interrupted => NodeStatus::Interrupted,
                ItemStatus::Cancelled => NodeStatus::Cancelled,
                _ => NodeStatus::Running,
            },
            counts_for_run: false,
            provider_thread_id: Some(turn.provider_thread_id),
            provider_turn_id: Some(turn.reference.provider_turn_id),
            native_item_ref: item.native_item_ref.clone(),
            runtime_request_id: None,
            checkpoint_scope_id: None,
            started_at: Some(turn.started_at),
            completed_at: status.is_terminal().then_some(now),
        };
        Some((item, node))
    }

    // -- dialogs: approvals and questions --------------------------------------------

    fn dialog(&mut self, frame: &Value, out: &mut Output) {
        let Some(method) = str_of(frame, "method") else {
            return;
        };
        if method == "cancel" {
            // The agent withdrew a dialog (a timeout, an abort).
            if let Some(target) = str_of(frame, "targetId")
                && let Some(pending) = self.dialogs.remove(target)
            {
                let events = self.close_dialog(pending.request, RequestStatus::Cancelled);
                out.events.extend(events);
            }
            return;
        }
        if !matches!(method, "confirm" | "select" | "input" | "editor") {
            // `notify`, `setStatus`, `setWidget`, … carry nothing to answer.
            return;
        }
        let Some(native) = str_of(frame, "id").map(str::to_owned) else {
            return;
        };
        let cancel = json!({"type": "extension_ui_response", "id": native, "cancelled": true});
        if !self.activity(out) {
            // No turn to show it in: answer it the way Pi does without a UI.
            out.writes.push(cancel);
            return;
        }
        let Some(turn) = self.turn.as_ref() else {
            return;
        };
        let flavor = self.scope.flavor;
        let pt = turn.provider_thread_id.0;
        let request_id: RuntimeRequestId = self.bind(pt, EntityKind::Request, &native);
        let title = str_of(frame, "title").unwrap_or(method).to_owned();
        let message = str_of(frame, "message").map(str::to_owned);
        let now = self.now();
        let approval = protocol::approval_tool(flavor, frame);
        let (kind, request_kind, options, questions) = match &approval {
            Some(tool) => {
                let request_kind = if protocol::is_file_change_tool(tool) {
                    RuntimeRequestKind::FileChangeApproval
                } else {
                    RuntimeRequestKind::CommandApproval
                };
                let (kind, options) = if method == "confirm" {
                    (
                        DialogKind::Confirm,
                        vec![
                            PermissionOption {
                                id: ALLOW.to_owned(),
                                label: "Allow once".to_owned(),
                                allow: true,
                            },
                            PermissionOption {
                                id: DENY.to_owned(),
                                label: "Deny".to_owned(),
                                allow: false,
                            },
                        ],
                    )
                } else {
                    (
                        DialogKind::Approval,
                        vec![
                            PermissionOption {
                                id: OMP_APPROVE.to_owned(),
                                label: OMP_APPROVE.to_owned(),
                                allow: true,
                            },
                            PermissionOption {
                                id: OMP_DENY.to_owned(),
                                label: OMP_DENY.to_owned(),
                                allow: false,
                            },
                        ],
                    )
                };
                (kind, request_kind, options, Vec::new())
            }
            None => {
                let options = if method == "select" {
                    frame
                        .get("options")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str)
                        .map(|option| UserInputOption {
                            label: option.to_owned(),
                            description: None,
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                let question = message
                    .clone()
                    .or_else(|| str_of(frame, "placeholder").map(str::to_owned))
                    .unwrap_or_else(|| title.clone());
                (
                    DialogKind::Question,
                    RuntimeRequestKind::UserInput,
                    Vec::new(),
                    vec![UserInputQuestion {
                        id: native.clone(),
                        header: title.clone(),
                        question,
                        options,
                        multi_select: false,
                    }],
                )
            }
        };
        let request = RuntimeRequest {
            id: request_id,
            thread_id: turn.run.thread_id,
            node_id: turn.run.root_node_id,
            provider_turn_id: Some(turn.reference.provider_turn_id),
            native_request_ref: Some(ProviderRef::strong(self.driver(), &native)),
            kind: request_kind,
            status: RequestStatus::Pending,
            response_capability: ResponseCapability::Live {
                provider_session_id: self.scope.session_id,
            },
            options,
            questions,
            response: None,
            created_at: now,
            resolved_at: None,
        };
        let mut events = Vec::new();
        if let Some(tool) = &approval {
            let prompt = approval_prompt(tool, &title, message.as_deref());
            if let Some(item) = self.item(
                derived::request_item(request_id),
                ItemStatus::Waiting,
                TurnItemKind::ApprovalRequest {
                    request_id,
                    request_kind,
                    prompt: Some(prompt),
                    app_name: None,
                    options: request.options.clone(),
                },
            ) {
                events.push(AdapterEvent::TurnItemUpdated { item });
            }
        }
        events.push(AdapterEvent::RuntimeRequestUpdated {
            request: request.clone(),
        });
        self.dialogs.insert(native, PendingDialog { request, kind });
        let events = self.offer(events);
        out.events.extend(events);
    }

    fn close_dialog(
        &mut self,
        request: RuntimeRequest,
        status: RequestStatus,
    ) -> Vec<AdapterEvent> {
        let now = self.now();
        let request = RuntimeRequest {
            status,
            resolved_at: Some(now),
            ..request
        };
        self.offer(vec![AdapterEvent::RuntimeRequestUpdated { request }])
    }

    /// The `extension_ui_response` for a reply, and the resolved request.
    pub fn respond(
        &mut self,
        native: &str,
        reply: &RuntimeReply,
    ) -> Option<(Value, Vec<AdapterEvent>)> {
        let pending = self.dialogs.remove(native)?;
        let mut frame = json!({"type": "extension_ui_response", "id": native});
        let mut status = RequestStatus::Resolved;
        match (pending.kind, reply) {
            (DialogKind::Confirm, RuntimeReply::Decision { option_id }) => {
                frame["confirmed"] = Value::Bool(option_id == ALLOW);
            }
            (DialogKind::Approval, RuntimeReply::Decision { option_id }) => {
                frame["value"] = Value::String(option_id.clone());
            }
            (DialogKind::Question, RuntimeReply::Answers(answers)) => {
                let answer = answers
                    .iter()
                    .find(|answer| answer.question_id == native)
                    .or_else(|| answers.first())
                    .and_then(|answer| answer.answers.first());
                match answer {
                    Some(value) => frame["value"] = Value::String(value.clone()),
                    None => {
                        frame["cancelled"] = Value::Bool(true);
                        status = RequestStatus::Cancelled;
                    }
                }
            }
            _ => {
                frame["cancelled"] = Value::Bool(true);
                status = RequestStatus::Cancelled;
            }
        }
        let events = self.close_dialog(pending.request, status);
        Some((frame, events))
    }

    // -- the turn's end ------------------------------------------------------------

    fn settle(&mut self, out: &mut Output) {
        let Some(turn) = self.turn.as_ref() else {
            return;
        };
        if !turn.accepted && !turn.active {
            // A settle before this prompt reached the agent belongs to
            // earlier work.
            return;
        }
        let outcome = if turn.interrupt_requested || turn.aborted {
            TurnOutcome::Interrupted
        } else if let Some(failure) = turn.failure.clone() {
            TurnOutcome::Failed { failure }
        } else {
            TurnOutcome::Completed
        };
        let summary = match &outcome {
            TurnOutcome::Failed { failure } => Some(failure.message.clone()),
            _ => None,
        };
        self.finish_turn(outcome, summary, out);
    }

    /// Ends the open turn with `outcome`: closes its streaming messages,
    /// open tools and dialogs, then the provider turn and exactly one
    /// terminal.
    pub fn finish_turn(&mut self, outcome: TurnOutcome, summary: Option<String>, out: &mut Output) {
        if self.turn.is_none() {
            return;
        }
        let item_status = match &outcome {
            TurnOutcome::Completed => ItemStatus::Completed,
            TurnOutcome::Interrupted => ItemStatus::Interrupted,
            TurnOutcome::Cancelled => ItemStatus::Cancelled,
            TurnOutcome::Failed { .. } => ItemStatus::Failed,
        };
        let open_messages = self
            .messages
            .iter()
            .filter(|(_, state)| !state.done)
            .map(|(seq, _)| *seq)
            .collect::<Vec<_>>();
        for seq in open_messages {
            self.complete_message(seq, item_status, out);
        }
        let open_tools = self
            .tools
            .iter()
            .filter(|(_, tool)| !tool.done)
            .map(|(seq, _)| *seq)
            .collect::<Vec<_>>();
        for seq in open_tools {
            let native = self
                .tool_index
                .iter()
                .find(|(_, value)| **value == seq)
                .map(|(native, _)| native.clone())
                .unwrap_or_default();
            if let Some(tool) = self.tools.get_mut(&seq) {
                tool.done = true;
            }
            if let Some((item, node)) = self.tool_rows(seq, &native, item_status, None, false) {
                let events = self.offer(vec![
                    AdapterEvent::NodeUpdated { node },
                    AdapterEvent::TurnItemUpdated { item },
                ]);
                out.events.extend(events);
            }
        }
        let dialogs = std::mem::take(&mut self.dialogs);
        for (native, pending) in dialogs {
            out.writes
                .push(json!({"type": "extension_ui_response", "id": native, "cancelled": true}));
            let events = self.close_dialog(pending.request, RequestStatus::Cancelled);
            out.events.extend(events);
        }
        let Some(turn) = self.turn.take() else {
            return;
        };
        if !turn.accepted {
            out.accepted = Some(Ok(()));
        }
        let status = match &outcome {
            TurnOutcome::Completed => ProviderTurnStatus::Completed,
            TurnOutcome::Interrupted => ProviderTurnStatus::Interrupted,
            TurnOutcome::Cancelled => ProviderTurnStatus::Cancelled,
            TurnOutcome::Failed { .. } => ProviderTurnStatus::Failed,
        };
        out.events.extend(self.coalescer.flush());
        out.events.push(AdapterEvent::ProviderTurnUpdated {
            turn: self.turn_snapshot(&turn, status),
        });
        out.events.push(AdapterEvent::TurnTerminal {
            terminal: TurnTerminal {
                run: turn.run,
                provider_turn: Some(turn.reference.clone()),
                outcome,
                thread_disposition: ThreadDisposition::Reusable,
                child_turns_continue: false,
                background_pending: false,
                summary,
            },
        });
        self.current_message = None;
        self.command_output = None;
        self.messages.clear();
        self.tools.clear();
        self.tool_index.clear();
        out.turn_ended = true;
    }

    /// Everything still held by the coalescer.
    pub fn flush(&mut self) -> Vec<AdapterEvent> {
        self.coalescer.flush()
    }
}

/// What an approval row shows: the command or path from Waku's extension's
/// JSON summary, else the dialog's text.
fn approval_prompt(tool: &str, title: &str, message: Option<&str>) -> String {
    if let Some(message) = message {
        if let Ok(input) = serde_json::from_str::<Value>(message)
            && let Some(text) = str_of(&input, "command").or_else(|| str_of(&input, "path"))
        {
            return text.to_owned();
        }
        return message.to_owned();
    }
    // Oh My Pi: "Allow tool: <name>" then the tool's own detail lines.
    let details = title.lines().skip(1).collect::<Vec<_>>().join("\n");
    if details.trim().is_empty() {
        format!("Run {tool}")
    } else {
        details.trim().to_owned()
    }
}

#[cfg(test)]
mod tests;

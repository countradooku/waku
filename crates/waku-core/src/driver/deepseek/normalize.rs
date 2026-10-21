//! Harness downlink envelopes → `AdapterEvent`s (RFC 0002 "DeepSeek
//! Harness").
//!
//! The session's actor feeds every envelope of its native session here in
//! downlink order and performs the side effects asked for. Session events
//! carry a `seq`; anything at or below the newest seq seen is a replay
//! (history catch-up overlapping the live stream) and is dropped. A turn is
//! the span from `turn/start` to `turn/end`, and the `turn/start` seq is its
//! strong native id.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde_json::Value;
use waku_protocol::orchestration::{self as orch, *};

use crate::driver::tool_items::tool_item;
use crate::driver::v2::coalescer::DeltaCoalescer;
use crate::driver::v2::{
    AdapterEvent, BackgroundWorkItem, Clock, EntityKind, IdAllocator, NativeRef, ProviderTurnRef,
    RunRef, ThreadDisposition, TurnOutcome, TurnTerminal,
};

const DRIVER: orch::DriverKind = orch::DriverKind::DeepSeek;

/// Harness approval outcomes Waku offers when a request lists none.
pub(super) const OUTCOME_ALLOW_ONCE: &str = "allowed-once";
pub(super) const OUTCOME_REJECT: &str = "rejected";

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Effect {
    /// Approve without asking (the runtime mode allows it).
    AutoApprove { rpc_id: String, approval_id: String },
    /// A question request with nothing to ask is refused.
    RejectQuestion { rpc_id: String },
    /// The Host has events past the newest one seen: read the history.
    CatchUp,
    /// The command list changed.
    CommandsChanged,
}

/// A prompt the Host accepted whose `turn/start` has not arrived.
pub(super) struct PendingTurn {
    pub run: RunRef,
    pub ordinal: u32,
    pub provider_thread_id: ProviderThreadId,
}

pub(super) struct OpenTurn {
    pub run: RunRef,
    pub reference: ProviderTurnRef,
    pub provider_thread_id: ProviderThreadId,
    pub started_at: Timestamp,
    pub interrupted: bool,
    pub usage: Option<TokenUsage>,
}

struct Part {
    text: String,
    reasoning: bool,
    started_at: Timestamp,
}

struct Tool {
    name: String,
    title: Option<String>,
    arguments: Value,
    started_at: Timestamp,
}

pub(super) enum Interaction {
    Approval {
        approval_id: String,
    },
    Question {
        questions: Vec<(String, HashSet<String>)>,
    },
}

pub(super) struct Pending {
    pub request: RuntimeRequest,
    pub interaction: Interaction,
}

pub(super) struct Normalizer {
    pub ids: Arc<dyn IdAllocator>,
    pub clock: Arc<dyn Clock>,
    pub provider_session_id: ProviderSessionId,
    pub instance_id: ProviderInstanceId,
    pub mode: RuntimeMode,
    pub pending_turn: Option<PendingTurn>,
    pub turn: Option<OpenTurn>,
    /// A turn cancelled before it started: its events are dropped until its
    /// `turn/end`.
    pub swallow: bool,
    pub last_seq: i64,
    /// `(turn/start seq, turn/end seq)` of completed turns, oldest first.
    pub completed: Vec<(u64, u64)>,
    pub requests: HashMap<String, Pending>,
    /// Live background jobs (not subagents), by id.
    pub jobs: HashSet<String>,
    pub provider_thread: Option<ProviderThreadId>,
    /// The latest run, for subagents reported between turns.
    last_run: Option<RunRef>,
    window: Option<u64>,
    parts: HashMap<String, Part>,
    streamed: HashSet<(u64, u64)>,
    tools: HashMap<String, Tool>,
    coalescer: DeltaCoalescer,
}

fn str_of<'a>(data: &'a Value, key: &str) -> Option<&'a str> {
    data.get(key).and_then(Value::as_str)
}

/// A turn's failure from `turn/end`'s `reason.error`. A usage limit is
/// recognized by its code and takes `reset_at` from the structured
/// `resetAt` (epoch ms) or `retryAfterMs`, never from the message.
pub(super) fn turn_failure(
    error: Option<&Value>,
    fallback: &str,
    now: Timestamp,
) -> ProviderFailure {
    let code = error
        .and_then(|error| str_of(error, "code").or_else(|| str_of(error, "kind")))
        .unwrap_or_default();
    let message = error
        .and_then(|error| str_of(error, "message"))
        .unwrap_or(fallback);
    let lower = code.to_ascii_lowercase();
    let class = if [
        "rate_limit",
        "rate-limit",
        "quota",
        "usage_limit",
        "insufficient_balance",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
    {
        FailureClass::UsageLimit
    } else if lower.contains("auth") || lower.contains("permission") {
        FailureClass::PermissionError
    } else if !lower.is_empty() && lower.starts_with("provider") {
        FailureClass::ProviderError
    } else {
        FailureClass::Unknown
    };
    let mut failure = ProviderFailure::new(class, message);
    if !code.is_empty() {
        failure = failure.with_code(code);
    }
    if class == FailureClass::UsageLimit {
        failure.retryable = Some(true);
        failure.reset_at = error
            .and_then(|error| error.get("resetAt"))
            .and_then(Value::as_i64)
            .or_else(|| {
                error
                    .and_then(|error| error.get("retryAfterMs"))
                    .and_then(Value::as_i64)
                    .map(|after| now + after)
            });
    }
    failure
}

fn approval_kind(tool: &str) -> RuntimeRequestKind {
    match waku_protocol::model::ActivityKind::from_tool_name(tool) {
        waku_protocol::model::ActivityKind::Command => RuntimeRequestKind::CommandApproval,
        waku_protocol::model::ActivityKind::FileChange => RuntimeRequestKind::FileChangeApproval,
        _ => RuntimeRequestKind::Permission,
    }
}

impl Normalizer {
    pub(super) fn new(
        ids: Arc<dyn IdAllocator>,
        clock: Arc<dyn Clock>,
        provider_session_id: ProviderSessionId,
        instance_id: ProviderInstanceId,
        mode: RuntimeMode,
    ) -> Self {
        Self {
            coalescer: DeltaCoalescer::new(clock.clone()),
            ids,
            clock,
            provider_session_id,
            instance_id,
            mode,
            pending_turn: None,
            turn: None,
            swallow: false,
            last_seq: -1,
            completed: Vec::new(),
            requests: HashMap::new(),
            jobs: HashSet::new(),
            provider_thread: None,
            last_run: None,
            window: None,
            parts: HashMap::new(),
            streamed: HashSet::new(),
            tools: HashMap::new(),
        }
    }

    pub(super) fn now(&self) -> Timestamp {
        self.clock.now()
    }

    fn bind<T: From<uuid::Uuid>>(
        &self,
        scope: ProviderThreadId,
        kind: EntityKind,
        native: &str,
    ) -> T {
        T::from(self.ids.bind(scope.0, kind, native))
    }

    pub(super) fn has_pending_snapshots(&self) -> bool {
        self.coalescer.has_pending()
    }

    pub(super) fn due(&mut self) -> Vec<AdapterEvent> {
        self.coalescer.due()
    }

    fn offer(&mut self, events: Vec<AdapterEvent>) -> Vec<AdapterEvent> {
        events
            .into_iter()
            .flat_map(|event| self.coalescer.offer(event))
            .collect()
    }

    /// Resets the stream position for a different native session.
    pub(super) fn follow(&mut self, last_seq: i64, completed: Vec<(u64, u64)>) {
        self.last_seq = last_seq;
        self.completed = completed;
    }

    pub(super) fn turn_snapshot(
        &self,
        turn: &OpenTurn,
        status: ProviderTurnStatus,
    ) -> ProviderTurn {
        let terminal = !matches!(
            status,
            ProviderTurnStatus::Running | ProviderTurnStatus::Pending
        );
        ProviderTurn {
            id: turn.reference.provider_turn_id,
            provider_thread_id: turn.provider_thread_id,
            node_id: turn.run.root_node_id,
            run_attempt_id: Some(turn.run.attempt_id),
            native_turn_ref: Some(turn.reference.native.to_provider_ref(DRIVER)),
            ordinal: turn.reference.ordinal,
            status,
            started_at: Some(turn.started_at),
            completed_at: terminal.then(|| self.now()),
            token_usage: turn.usage.clone(),
            turn_token_usage: None,
        }
    }

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

    /// Ends the open turn (or a pending one that never started) with its
    /// one terminal.
    pub(super) fn finish_turn(
        &mut self,
        outcome: TurnOutcome,
        disposition: ThreadDisposition,
    ) -> Vec<AdapterEvent> {
        let mut events = self.coalescer.flush();
        let status = match &outcome {
            TurnOutcome::Completed => ProviderTurnStatus::Completed,
            TurnOutcome::Interrupted => ProviderTurnStatus::Interrupted,
            TurnOutcome::Cancelled => ProviderTurnStatus::Cancelled,
            TurnOutcome::Failed { .. } => ProviderTurnStatus::Failed,
        };
        let (run, provider_turn) = if let Some(turn) = self.turn.take() {
            events.push(AdapterEvent::ProviderTurnUpdated {
                turn: self.turn_snapshot(&turn, status),
            });
            (turn.run, Some(turn.reference))
        } else if let Some(pending) = self.pending_turn.take() {
            (pending.run, None)
        } else {
            return events;
        };
        events.push(AdapterEvent::TurnTerminal {
            terminal: TurnTerminal {
                run,
                provider_turn,
                outcome,
                thread_disposition: disposition,
                child_turns_continue: false,
                background_pending: false,
                summary: None,
            },
        });
        self.parts.clear();
        self.tools.clear();
        self.streamed.clear();
        events
    }

    /// One downlink envelope of this session.
    pub(super) fn on_envelope(&mut self, envelope: &Value) -> (Vec<AdapterEvent>, Vec<Effect>) {
        let payload = envelope.get("payload").unwrap_or(envelope);
        let rpc_id = str_of(envelope, "rpcId").unwrap_or_default().to_owned();
        let mut effects = Vec::new();
        let events = match str_of(payload, "type").unwrap_or_default() {
            "session/event" => match payload.get("event") {
                Some(event) => self.session_event(event, payload.get("view")),
                None => Vec::new(),
            },
            "session/subscribed" => {
                let remote = payload.get("lastSeq").and_then(Value::as_i64).unwrap_or(-1);
                if remote > self.last_seq {
                    effects.push(Effect::CatchUp);
                }
                Vec::new()
            }
            "approval/requested" => self.approval(&rpc_id, payload, &mut effects),
            "approval/resolved" => {
                let approval = str_of(payload, "approvalId").unwrap_or_default();
                let rpc = self
                    .requests
                    .iter()
                    .find(|(_, pending)| {
                        matches!(&pending.interaction, Interaction::Approval { approval_id } if approval_id == approval)
                    })
                    .map(|(rpc, _)| rpc.clone());
                match rpc {
                    Some(rpc) => self.resolve(&rpc, RequestStatus::Resolved),
                    None => Vec::new(),
                }
            }
            "question/requested" => self.question(&rpc_id, payload, &mut effects),
            "question/resolved" => match str_of(payload, "questionRpcId") {
                Some(rpc) => self.resolve(rpc, RequestStatus::Resolved),
                None => Vec::new(),
            },
            "session/jobs" => self.jobs(payload),
            "session/projection" => match (str_of(payload, "key"), payload.get("value")) {
                (Some(key), Some(value)) => self.projection(key, value),
                _ => Vec::new(),
            },
            "host/remote-event" if str_of(payload, "event") == Some("commands/change") => {
                effects.push(Effect::CommandsChanged);
                Vec::new()
            }
            _ => Vec::new(),
        };
        (events, effects)
    }

    /// A stored or live session event.
    pub(super) fn session_event(
        &mut self,
        event: &Value,
        view: Option<&Value>,
    ) -> Vec<AdapterEvent> {
        let Some(seq) = event.get("seq").and_then(Value::as_u64) else {
            return Vec::new();
        };
        if i64::try_from(seq).is_ok_and(|seq| seq <= self.last_seq) {
            return Vec::new();
        }
        self.last_seq = i64::try_from(seq).unwrap_or(i64::MAX);
        let data = event.get("data").unwrap_or(&Value::Null);
        let kind = str_of(event, "type").unwrap_or_default();
        if kind == "turn/end" {
            let start = self
                .turn
                .as_ref()
                .and_then(|turn| turn.reference.native.id.parse().ok())
                .unwrap_or(seq);
            self.completed.push((start, seq));
        }
        if self.swallow {
            if kind == "turn/end" {
                self.swallow = false;
            }
            return Vec::new();
        }
        match kind {
            "turn/start" => self.turn_started(seq),
            "turn/end" => self.turn_ended(data),
            "assistant/chunk" => self.chunk(data),
            "assistant/message" => self.message(data),
            "tool/call" => self.tool_call(data, view),
            "tool/result" => self.tool_result(data, view),
            "todo/write" => self.todos(data),
            "request/context" => {
                self.window = data
                    .get("contextWindow")
                    .and_then(Value::as_u64)
                    .filter(|window| *window > 0);
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    fn turn_started(&mut self, seq: u64) -> Vec<AdapterEvent> {
        let Some(pending) = self.pending_turn.take() else {
            // A turn the Host started on its own; the orchestrator decides
            // who owns it (not reported until #59).
            return Vec::new();
        };
        let native = seq.to_string();
        let turn = OpenTurn {
            run: pending.run,
            reference: ProviderTurnRef {
                provider_turn_id: self.bind(
                    pending.provider_thread_id,
                    EntityKind::ProviderTurn,
                    &native,
                ),
                native: NativeRef::strong(native),
                ordinal: pending.ordinal,
            },
            provider_thread_id: pending.provider_thread_id,
            started_at: self.now(),
            interrupted: false,
            usage: None,
        };
        self.last_run = Some(turn.run);
        let snapshot = self.turn_snapshot(&turn, ProviderTurnStatus::Running);
        self.turn = Some(turn);
        self.offer(vec![AdapterEvent::ProviderTurnUpdated { turn: snapshot }])
    }

    fn turn_ended(&mut self, data: &Value) -> Vec<AdapterEvent> {
        let reason = data.pointer("/reason/kind").and_then(Value::as_str);
        let now = self.now();
        let outcome = match reason {
            Some("completed" | "max-tokens") => TurnOutcome::Completed,
            Some("aborted") => TurnOutcome::Interrupted,
            Some("error") => TurnOutcome::Failed {
                failure: turn_failure(
                    data.pointer("/reason/error"),
                    "DeepSeek Harness turn failed",
                    now,
                ),
            },
            Some("blocked") => TurnOutcome::Failed {
                failure: ProviderFailure::new(
                    FailureClass::PermissionError,
                    "DeepSeek Harness blocked the turn",
                )
                .with_code("blocked"),
            },
            Some("interrupted") => TurnOutcome::Failed {
                failure: ProviderFailure::new(
                    FailureClass::ProviderError,
                    "DeepSeek Harness recovered an interrupted turn",
                )
                .with_code("interrupted"),
            },
            other => TurnOutcome::Failed {
                failure: ProviderFailure::new(
                    FailureClass::Unknown,
                    format!(
                        "DeepSeek Harness ended the turn: {}",
                        other.unwrap_or("no reason")
                    ),
                ),
            },
        };
        self.finish_turn(outcome, ThreadDisposition::Reusable)
    }

    fn part_snapshot(&self, key: &str, streaming: bool) -> Vec<AdapterEvent> {
        let (Some(turn), Some(part)) = (self.turn.as_ref(), self.parts.get(key)) else {
            return Vec::new();
        };
        let now = self.now();
        let pt = turn.provider_thread_id;
        let item_id: TurnItemId = self.bind(pt, EntityKind::Item, key);
        let status = if streaming {
            ItemStatus::Running
        } else {
            ItemStatus::Completed
        };
        let native = Some(NativeRef::strong(key).to_provider_ref(DRIVER));
        if part.reasoning {
            let mut item = Self::item(
                turn,
                item_id,
                now,
                status,
                TurnItemKind::Reasoning {
                    text: part.text.clone(),
                    streaming,
                },
            );
            item.started_at = Some(part.started_at);
            item.native_item_ref = native;
            return vec![AdapterEvent::TurnItemUpdated { item }];
        }
        let message_id: MessageId = self.bind(pt, EntityKind::Message, key);
        let mut item = Self::item(
            turn,
            item_id,
            now,
            status,
            TurnItemKind::AssistantMessage {
                message_id,
                text: part.text.clone(),
                streaming,
            },
        );
        item.started_at = Some(part.started_at);
        item.native_item_ref = native;
        vec![
            AdapterEvent::MessageUpdated {
                message: Message {
                    id: message_id,
                    thread_id: turn.run.thread_id,
                    run_id: Some(turn.run.run_id),
                    node_id: Some(turn.run.root_node_id),
                    role: MessageRole::Assistant,
                    created_by: Actor::Agent,
                    creation_source: CreationSource::Provider,
                    text: part.text.clone(),
                    display_text: None,
                    attachments: Vec::new(),
                    context: Vec::new(),
                    notification: None,
                    sender_thread_id: None,
                    streaming,
                    created_at: part.started_at,
                    updated_at: now,
                },
            },
            AdapterEvent::TurnItemUpdated { item },
        ]
    }

    fn append(&mut self, turn: u64, step: u64, reasoning: bool, text: &str) -> Vec<AdapterEvent> {
        let key = format!(
            "{turn}:{step}:{}",
            if reasoning { "reasoning" } else { "text" }
        );
        let now = self.now();
        self.parts
            .entry(key.clone())
            .or_insert(Part {
                text: String::new(),
                reasoning,
                started_at: now,
            })
            .text
            .push_str(text);
        let events = self.part_snapshot(&key, true);
        self.offer(events)
    }

    fn usage(&mut self, usage: Option<&Value>) -> Vec<AdapterEvent> {
        let Some(usage) = usage else {
            return Vec::new();
        };
        let count = |key: &str| usage.get(key).and_then(Value::as_u64).unwrap_or(0);
        let input = count("inputTokens");
        let output = count("outputTokens");
        let cache_read = count("cacheReadTokens");
        let cache_write = count("cacheWriteTokens");
        let used = input + output + cache_read + cache_write;
        if used == 0 {
            return Vec::new();
        }
        let now = self.now();
        let window = self.window;
        let Some(turn) = self.turn.as_mut() else {
            return Vec::new();
        };
        turn.usage = Some(TokenUsage {
            used,
            max: window,
            input: Some(input + cache_read + cache_write),
            cached_input: Some(cache_read),
            output: Some(output),
            reasoning_output: None,
            prompt_cache_ttl_ms: None,
            updated_at: now,
        });
        let turn = self.turn.as_ref().expect("checked above");
        let snapshot = self.turn_snapshot(turn, ProviderTurnStatus::Running);
        self.offer(vec![AdapterEvent::ProviderTurnUpdated { turn: snapshot }])
    }

    fn chunk(&mut self, data: &Value) -> Vec<AdapterEvent> {
        let chunk = data.get("chunk").unwrap_or(&Value::Null);
        let turn = data.get("turn").and_then(Value::as_u64).unwrap_or(0);
        let step = data.get("step").and_then(Value::as_u64).unwrap_or(0);
        match str_of(chunk, "type") {
            Some(kind @ ("text-delta" | "reasoning-delta")) => {
                self.streamed.insert((turn, step));
                match str_of(chunk, "text") {
                    Some(text) => self.append(turn, step, kind == "reasoning-delta", text),
                    None => Vec::new(),
                }
            }
            Some("usage") => self.usage(chunk.get("usage")),
            _ => Vec::new(),
        }
    }

    fn message(&mut self, data: &Value) -> Vec<AdapterEvent> {
        let turn = data.get("turn").and_then(Value::as_u64).unwrap_or(0);
        let step = data.get("step").and_then(Value::as_u64).unwrap_or(0);
        let mut events = Vec::new();
        if !self.streamed.remove(&(turn, step)) {
            for block in data
                .pointer("/message/content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                match (str_of(block, "type"), str_of(block, "text")) {
                    (Some("text"), Some(text)) => {
                        events.extend(self.append(turn, step, false, text))
                    }
                    (Some("reasoning"), Some(text)) => {
                        events.extend(self.append(turn, step, true, text));
                    }
                    _ => {}
                }
            }
        }
        // The message is complete: its parts stop streaming.
        for kind in ["reasoning", "text"] {
            let key = format!("{turn}:{step}:{kind}");
            if self.parts.contains_key(&key) {
                let snapshot = self.part_snapshot(&key, false);
                events.extend(self.offer(snapshot));
            }
        }
        events.extend(self.usage(data.get("usage")));
        events
    }

    fn tool_snapshot(
        &self,
        id: &str,
        output: Option<&Value>,
        status: ItemStatus,
    ) -> Vec<AdapterEvent> {
        let (Some(turn), Some(tool)) = (self.turn.as_ref(), self.tools.get(id)) else {
            return Vec::new();
        };
        let now = self.now();
        let pt = turn.provider_thread_id;
        let item_id: TurnItemId = self.bind(pt, EntityKind::Item, id);
        let node_id: NodeId = self.bind(pt, EntityKind::Node, id);
        let normalized = tool_item(
            &tool.name,
            tool.title.clone(),
            Some(&tool.arguments),
            output,
            status == ItemStatus::Failed,
            None,
        );
        let mut item = Self::item(turn, item_id, now, status, normalized.kind);
        item.title = Some(normalized.title);
        item.node_id = Some(node_id);
        item.started_at = Some(tool.started_at);
        item.native_item_ref = Some(ProviderRef::strong(DRIVER, id));
        let node = ExecutionNode {
            id: node_id,
            thread_id: turn.run.thread_id,
            run_id: Some(turn.run.run_id),
            parent_node_id: Some(turn.run.root_node_id),
            root_node_id: turn.run.root_node_id,
            kind: NodeKind::ToolCall,
            status: match status {
                ItemStatus::Completed => NodeStatus::Completed,
                ItemStatus::Failed => NodeStatus::Failed,
                _ => NodeStatus::Running,
            },
            counts_for_run: false,
            provider_thread_id: Some(pt),
            provider_turn_id: Some(turn.reference.provider_turn_id),
            native_item_ref: item.native_item_ref.clone(),
            runtime_request_id: None,
            checkpoint_scope_id: None,
            started_at: Some(tool.started_at),
            completed_at: status.is_terminal().then_some(now),
        };
        vec![
            AdapterEvent::NodeUpdated { node },
            AdapterEvent::TurnItemUpdated { item },
        ]
    }

    fn tool_call(&mut self, data: &Value, view: Option<&Value>) -> Vec<AdapterEvent> {
        let Some(id) = str_of(data, "callId") else {
            return Vec::new();
        };
        let arguments = str_of(data, "arguments")
            .and_then(|arguments| serde_json::from_str(arguments).ok())
            .unwrap_or_else(|| data.get("arguments").cloned().unwrap_or(Value::Null));
        let title = view
            .and_then(|view| view.pointer("/view/title"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let started_at = self.now();
        self.tools.insert(
            id.to_owned(),
            Tool {
                name: str_of(data, "name").unwrap_or("tool").to_owned(),
                title,
                arguments,
                started_at,
            },
        );
        let events = self.tool_snapshot(id, None, ItemStatus::Running);
        self.offer(events)
    }

    fn tool_result(&mut self, data: &Value, view: Option<&Value>) -> Vec<AdapterEvent> {
        let Some(id) = data
            .pointer("/message/source/callId")
            .or_else(|| data.pointer("/message/content/0/toolCallId"))
            .and_then(Value::as_str)
        else {
            return Vec::new();
        };
        let id = id.to_owned();
        if let Some(title) = view
            .and_then(|view| view.pointer("/view/title"))
            .and_then(Value::as_str)
            && let Some(tool) = self.tools.get_mut(&id)
        {
            tool.title = Some(title.to_owned());
        }
        let failed = data.get("error").is_some_and(|error| !error.is_null())
            || data
                .pointer("/message/content/0/isError")
                .and_then(Value::as_bool)
                .unwrap_or(false);
        let text = data
            .pointer("/message/content")
            .and_then(Value::as_array)
            .map(|blocks| {
                blocks
                    .iter()
                    .filter_map(|block| {
                        str_of(block, "text").or_else(|| {
                            block
                                .get("content")
                                .and_then(Value::as_array)
                                .and_then(|inner| inner.first())
                                .and_then(|inner| str_of(inner, "text"))
                        })
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .map(Value::String);
        let status = if failed {
            ItemStatus::Failed
        } else {
            ItemStatus::Completed
        };
        let events = self.tool_snapshot(&id, text.as_ref(), status);
        self.tools.remove(&id);
        self.offer(events)
    }

    fn todos(&mut self, data: &Value) -> Vec<AdapterEvent> {
        let Some(turn) = self.turn.as_ref() else {
            return Vec::new();
        };
        let steps = data
            .get("todos")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
            .map(|(index, todo)| PlanStep {
                id: str_of(todo, "id")
                    .map(str::to_owned)
                    .unwrap_or_else(|| index.to_string()),
                text: str_of(todo, "content")
                    .or_else(|| str_of(todo, "text"))
                    .or_else(|| str_of(todo, "title"))
                    .unwrap_or_default()
                    .to_owned(),
                status: match str_of(todo, "status") {
                    Some("completed" | "done") => PlanStepStatus::Completed,
                    Some("in_progress" | "in-progress" | "running") => PlanStepStatus::Running,
                    _ => PlanStepStatus::Pending,
                },
            })
            .collect();
        let plan = Plan {
            id: self.bind(
                turn.provider_thread_id,
                EntityKind::Plan,
                &format!("todo:{}", turn.reference.native.id),
            ),
            thread_id: turn.run.thread_id,
            run_id: Some(turn.run.run_id),
            node_id: turn.run.root_node_id,
            status: PlanStatus::Active,
            kind: PlanKind::TodoList {
                steps,
                explanation: None,
            },
        };
        self.offer(vec![AdapterEvent::PlanUpdated { plan }])
    }

    fn request_snapshot(
        &self,
        turn: &OpenTurn,
        rpc_id: &str,
        kind: RuntimeRequestKind,
        options: Vec<PermissionOption>,
        questions: Vec<UserInputQuestion>,
    ) -> RuntimeRequest {
        RuntimeRequest {
            id: self.bind(turn.provider_thread_id, EntityKind::Request, rpc_id),
            thread_id: turn.run.thread_id,
            node_id: turn.run.root_node_id,
            provider_turn_id: Some(turn.reference.provider_turn_id),
            native_request_ref: Some(ProviderRef::strong(DRIVER, rpc_id)),
            kind,
            status: RequestStatus::Pending,
            response_capability: ResponseCapability::Live {
                provider_session_id: self.provider_session_id,
            },
            options,
            questions,
            response: None,
            created_at: self.now(),
            resolved_at: None,
        }
    }

    /// The outcomes a request offers, passed through as option ids; the two
    /// Waku has always sent when it lists none.
    fn approval_options(payload: &Value) -> Vec<PermissionOption> {
        let listed = payload
            .get("options")
            .or_else(|| payload.get("outcomes"))
            .and_then(Value::as_array)
            .map(|options| {
                options
                    .iter()
                    .filter_map(|option| {
                        let id = option
                            .as_str()
                            .or_else(|| str_of(option, "outcome"))
                            .or_else(|| str_of(option, "id"))?;
                        let label = str_of(option, "label").unwrap_or(id);
                        Some(PermissionOption {
                            id: id.to_owned(),
                            label: label.to_owned(),
                            allow: id.starts_with("allow"),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if !listed.is_empty() {
            return listed;
        }
        vec![
            PermissionOption {
                id: OUTCOME_ALLOW_ONCE.to_owned(),
                label: "Allow once".to_owned(),
                allow: true,
            },
            PermissionOption {
                id: OUTCOME_REJECT.to_owned(),
                label: "Deny".to_owned(),
                allow: false,
            },
        ]
    }

    fn approval(
        &mut self,
        rpc_id: &str,
        payload: &Value,
        effects: &mut Vec<Effect>,
    ) -> Vec<AdapterEvent> {
        let Some(approval_id) = str_of(payload, "approvalId") else {
            return Vec::new();
        };
        if rpc_id.is_empty() || self.requests.contains_key(rpc_id) {
            return Vec::new();
        }
        if self.mode != RuntimeMode::Ask {
            effects.push(Effect::AutoApprove {
                rpc_id: rpc_id.to_owned(),
                approval_id: approval_id.to_owned(),
            });
            return Vec::new();
        }
        let Some(turn) = self.turn.as_ref() else {
            return Vec::new();
        };
        let tool = str_of(payload, "toolName").unwrap_or("tool");
        let kind = approval_kind(tool);
        let options = Self::approval_options(payload);
        let request = self.request_snapshot(turn, rpc_id, kind, options.clone(), Vec::new());
        let now = self.now();
        let mut item = Self::item(
            turn,
            waku_orchestrator::provider::derived::request_item(request.id),
            now,
            ItemStatus::Waiting,
            TurnItemKind::ApprovalRequest {
                request_id: request.id,
                request_kind: kind,
                prompt: str_of(payload, "reason")
                    .map(str::to_owned)
                    .or_else(|| Some(format!("Allow {tool}?"))),
                app_name: None,
                options,
            },
        );
        item.title = Some(tool.to_owned());
        self.requests.insert(
            rpc_id.to_owned(),
            Pending {
                request: request.clone(),
                interaction: Interaction::Approval {
                    approval_id: approval_id.to_owned(),
                },
            },
        );
        self.offer(vec![
            AdapterEvent::TurnItemUpdated { item },
            AdapterEvent::RuntimeRequestUpdated { request },
        ])
    }

    fn question(
        &mut self,
        rpc_id: &str,
        payload: &Value,
        effects: &mut Vec<Effect>,
    ) -> Vec<AdapterEvent> {
        if rpc_id.is_empty() || self.requests.contains_key(rpc_id) {
            return Vec::new();
        }
        let mut answers = Vec::new();
        let questions = payload
            .get("questions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
            .filter_map(|(index, question)| {
                let text = str_of(question, "question").unwrap_or_default().trim();
                if text.is_empty() {
                    return None;
                }
                let id = str_of(question, "id")
                    .filter(|id| !id.is_empty())
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("question-{index}"));
                let options = question
                    .get("options")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|option| {
                        let label = str_of(option, "label")?.trim();
                        (!label.is_empty()).then(|| UserInputOption {
                            label: label.to_owned(),
                            description: str_of(option, "description")
                                .or_else(|| str_of(option, "detail"))
                                .map(str::trim)
                                .filter(|text| !text.is_empty())
                                .map(str::to_owned),
                        })
                    })
                    .collect::<Vec<_>>();
                answers.push((
                    id.clone(),
                    options.iter().map(|option| option.label.clone()).collect(),
                ));
                let detail = str_of(question, "detail")
                    .map(str::trim)
                    .filter(|detail| !detail.is_empty());
                Some(UserInputQuestion {
                    id,
                    header: str_of(question, "header")
                        .filter(|header| !header.trim().is_empty())
                        .map(str::to_owned)
                        .unwrap_or_else(|| format!("Question {}", index + 1)),
                    question: detail
                        .map(|detail| format!("{text}\n\n{detail}"))
                        .unwrap_or_else(|| text.to_owned()),
                    options,
                    multi_select: question
                        .get("multiSelect")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                })
            })
            .collect::<Vec<_>>();
        if questions.is_empty() {
            effects.push(Effect::RejectQuestion {
                rpc_id: rpc_id.to_owned(),
            });
            return Vec::new();
        }
        let Some(turn) = self.turn.as_ref() else {
            return Vec::new();
        };
        let request = self.request_snapshot(
            turn,
            rpc_id,
            RuntimeRequestKind::UserInput,
            Vec::new(),
            questions,
        );
        self.requests.insert(
            rpc_id.to_owned(),
            Pending {
                request: request.clone(),
                interaction: Interaction::Question { questions: answers },
            },
        );
        self.offer(vec![AdapterEvent::RuntimeRequestUpdated { request }])
    }

    pub(super) fn resolve(&mut self, rpc_id: &str, status: RequestStatus) -> Vec<AdapterEvent> {
        let Some(pending) = self.requests.remove(rpc_id) else {
            return Vec::new();
        };
        let now = self.now();
        self.offer(vec![AdapterEvent::RuntimeRequestUpdated {
            request: RuntimeRequest {
                status,
                resolved_at: Some(now),
                ..pending.request
            },
        }])
    }

    /// `session/jobs`: subagents become `SubagentUpdated`, everything else
    /// background work that outlives the turn.
    fn jobs(&mut self, payload: &Value) -> Vec<AdapterEvent> {
        let Some(pt) = self
            .turn
            .as_ref()
            .map(|turn| turn.provider_thread_id)
            .or(self.provider_thread)
        else {
            return Vec::new();
        };
        let run = self.turn.as_ref().map(|turn| turn.run).or(self.last_run);
        let now = self.now();
        let mut events = self.coalescer.flush();
        for job in payload
            .get("jobs")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(id) = str_of(job, "id") else {
                continue;
            };
            let kind = str_of(job, "kind").unwrap_or("process");
            let status = str_of(job, "status").unwrap_or("running");
            let live = matches!(status, "running" | "stopping");
            let label = str_of(job, "label").unwrap_or(kind);
            if kind.contains("subagent") {
                let Some(run) = run else { continue };
                let node: NodeId = self.bind(pt, EntityKind::Subagent, id);
                events.push(AdapterEvent::SubagentUpdated {
                    subagent: Subagent {
                        id: node,
                        thread_id: run.thread_id,
                        run_id: Some(run.run_id),
                        parent_node_id: run.root_node_id,
                        origin: SubagentOrigin::ProviderNative,
                        created_by: Actor::Agent,
                        driver: DRIVER,
                        provider_instance_id: self.instance_id.clone(),
                        provider_thread_id: None,
                        child_thread_id: None,
                        native_task_ref: Some(ProviderRef::strong(DRIVER, id)),
                        prompt: label.to_owned(),
                        title: Some(label.to_owned()),
                        model: None,
                        status: match status {
                            "completed" => SubagentStatus::Completed,
                            "failed" => SubagentStatus::Failed,
                            "killed" => SubagentStatus::Cancelled,
                            _ => SubagentStatus::Running,
                        },
                        progress: str_of(job, "detail").map(str::to_owned),
                        result: None,
                        completion_wake: None,
                        completion_delivery: None,
                        started_at: Some(now),
                        completed_at: (!live).then_some(now),
                        updated_at: now,
                    },
                });
                continue;
            }
            if live {
                self.jobs.insert(id.to_owned());
            } else {
                self.jobs.remove(id);
            }
            events.push(AdapterEvent::BackgroundWorkUpdated {
                work: BackgroundWorkItem {
                    provider_thread_id: pt,
                    task: PendingBackgroundTask {
                        kind: if kind.contains("monitor") {
                            PendingTaskKind::Monitor
                        } else {
                            PendingTaskKind::Command
                        },
                        task_id: id.to_owned(),
                        description: Some(label.to_owned()),
                        parent_task_id: None,
                        child_thread_id: None,
                    },
                    live,
                },
            });
        }
        events
    }

    /// `contextPressure` is the Host's own context-window gauge.
    fn projection(&mut self, key: &str, value: &Value) -> Vec<AdapterEvent> {
        if key != "contextPressure" {
            return Vec::new();
        }
        if let Some(window) = value.get("contextWindow").and_then(Value::as_u64) {
            self.window = Some(window);
        }
        let tokens = value
            .get("projectedTokens")
            .or_else(|| value.get("pressureTokens"))
            .and_then(Value::as_u64);
        let now = self.now();
        let window = self.window;
        let (Some(tokens), Some(turn)) = (tokens, self.turn.as_mut()) else {
            return Vec::new();
        };
        let mut usage = turn.usage.clone().unwrap_or(TokenUsage {
            used: 0,
            max: None,
            input: None,
            cached_input: None,
            output: None,
            reasoning_output: None,
            prompt_cache_ttl_ms: None,
            updated_at: now,
        });
        usage.used = tokens;
        usage.max = window;
        usage.updated_at = now;
        turn.usage = Some(usage);
        let turn = self.turn.as_ref().expect("checked above");
        let snapshot = self.turn_snapshot(turn, ProviderTurnStatus::Running);
        self.offer(vec![AdapterEvent::ProviderTurnUpdated { turn: snapshot }])
    }
}

/// The structured answer a question request takes: options by label,
/// anything else as custom text.
pub(super) fn question_answers(
    questions: &[(String, HashSet<String>)],
    answers: &[UserInputAnswer],
) -> Vec<Value> {
    questions
        .iter()
        .map(|(id, labels)| {
            let given = answers
                .iter()
                .find(|answer| &answer.question_id == id)
                .map(|answer| answer.answers.as_slice())
                .unwrap_or_default();
            let selected = given
                .iter()
                .filter(|answer| labels.contains(answer.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            let custom = given
                .iter()
                .filter(|answer| !labels.contains(answer.as_str()))
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            let mut answer = serde_json::json!({ "id": id, "selected": selected });
            if !custom.is_empty() {
                answer["custom"] = Value::String(custom);
            }
            answer
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn usage_limits_take_the_structured_reset_time() {
        let failure = turn_failure(
            Some(
                &json!({"code": "rate_limit_exceeded", "message": "slow down", "resetAt": 1_767_229_200_000_i64}),
            ),
            "failed",
            0,
        );
        assert_eq!(failure.class, FailureClass::UsageLimit);
        assert_eq!(failure.reset_at, Some(1_767_229_200_000));
        let failure = turn_failure(
            Some(&json!({"code": "quota", "retryAfterMs": 60_000})),
            "failed",
            1_000,
        );
        assert_eq!(failure.reset_at, Some(61_000));
        let failure = turn_failure(Some(&json!({"message": "boom"})), "failed", 0);
        assert_eq!(
            (failure.class, failure.reset_at),
            (FailureClass::Unknown, None)
        );
    }

    #[test]
    fn question_answers_separate_options_from_custom_text() {
        let questions = vec![(
            "q1".to_owned(),
            HashSet::from(["Blue".to_owned(), "Red".to_owned()]),
        )];
        let answers = vec![UserInputAnswer {
            question_id: "q1".to_owned(),
            answers: vec!["Blue".to_owned(), "teal".to_owned()],
        }];
        assert_eq!(
            question_answers(&questions, &answers),
            [json!({"id": "q1", "selected": ["Blue"], "custom": "teal"})]
        );
    }

    #[test]
    fn listed_outcomes_pass_through_as_option_ids() {
        let options = Normalizer::approval_options(
            &json!({"options": ["allowed-once", "allowed-always", "rejected"]}),
        );
        assert_eq!(
            options
                .iter()
                .map(|option| (option.id.as_str(), option.allow))
                .collect::<Vec<_>>(),
            [
                ("allowed-once", true),
                ("allowed-always", true),
                ("rejected", false)
            ]
        );
        let defaults = Normalizer::approval_options(&json!({}));
        assert_eq!(defaults[0].id, OUTCOME_ALLOW_ONCE);
        assert_eq!(defaults[1].id, OUTCOME_REJECT);
    }
}

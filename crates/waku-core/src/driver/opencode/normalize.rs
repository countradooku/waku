//! `GET /api/event` envelopes → `AdapterEvent`s (RFC 0002 "OpenCode").
//!
//! The session's actor feeds every envelope of its native session here, in
//! stream order, and performs the side effects this module asks for (an
//! automatic permission reply, a deferred interrupt). Everything is a
//! snapshot keyed by app ids bound through `ids`, so replaying a frame binds
//! the same ids.
//!
//! The traps the v1 driver documented still hold:
//! * `session.execution.{succeeded,failed,interrupted}` is the terminal; the
//!   service never publishes `session.idle`, and settling is idempotent, so a
//!   steered message that joins the running execution still ends one turn.
//! * An `interrupted` execution with reason `shutdown` is not the end of the
//!   turn: the service resumes the run when it boots again.
//! * Provider control markers (`providerState`, `resultState`,
//!   `providerResultState`) never reach a transcript.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;

use waku_protocol::orchestration::{self as orch, *};

use crate::driver::support::{OpenCodePermissionRequest, OpenCodePermissionState};
use crate::driver::tool_items::tool_item;
use crate::driver::v2::coalescer::DeltaCoalescer;
use crate::driver::v2::{
    AdapterEvent, BackgroundWorkItem, Clock, EntityKind, IdAllocator, NativeRef, ProviderTurnRef,
    RunRef, ThreadDisposition, TurnOutcome, TurnTerminal,
};
use crate::opencode_api::{FormField, FormInfo, ModelRef};

const PROVIDER_STATE_KEYS: [&str; 3] = ["providerState", "resultState", "providerResultState"];
const DRIVER: orch::DriverKind = orch::DriverKind::OpenCode;

/// Native option ids of a permission card. OpenCode's own reply values;
/// `always` is kept in driver state and sent as `once`.
pub(super) const OPTION_ONCE: &str = "once";
pub(super) const OPTION_ALWAYS: &str = "always";
pub(super) const OPTION_REJECT: &str = "reject";

/// What the actor must do after an event.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Effect {
    /// Answer a permission `once` without asking (runtime mode or a
    /// remembered `always`).
    AutoReply { request: String },
    /// Stop a run the user stopped while it could not be interrupted.
    Interrupt,
}

/// The turn this session is running.
pub(super) struct OpenTurn {
    pub run: RunRef,
    pub reference: ProviderTurnRef,
    pub provider_thread_id: ProviderThreadId,
    pub started_at: Timestamp,
    /// The user asked to stop it.
    pub interrupted: bool,
    pub usage: Option<TokenUsage>,
    /// The latest failed step's message.
    pub step_error: Option<String>,
}

struct Part {
    text: String,
    reasoning: bool,
    started_at: Timestamp,
}

struct Tool {
    name: String,
    input: Option<Value>,
    started_at: Timestamp,
}

/// A request the user has to answer.
pub(super) struct Pending {
    pub request: RuntimeRequest,
    /// A form's fields: an answer round-trips each field's own `key`.
    pub fields: Option<Vec<FormField>>,
}

pub(super) struct Normalizer {
    pub ids: Arc<dyn IdAllocator>,
    pub clock: Arc<dyn Clock>,
    pub provider_session_id: ProviderSessionId,
    pub mode: RuntimeMode,
    pub turn: Option<OpenTurn>,
    pub step_message: Option<String>,
    pub step_model: Option<ModelRef>,
    pub model: Option<ModelRef>,
    pub context_windows: HashMap<String, u64>,
    pub permissions: OpenCodePermissionState,
    pub pending: HashMap<String, Pending>,
    /// Requests found pending on resume, surfaced with the next turn.
    pub stranded: Vec<Value>,
    pub stop_pending: bool,
    parts: HashMap<String, Part>,
    tools: HashMap<String, Tool>,
    coalescer: DeltaCoalescer,
    /// Background shells by id, for the thread they belong to.
    pub shells: HashMap<String, bool>,
    /// The provider thread this session drives; background work outside a
    /// turn belongs to it.
    pub provider_thread: Option<ProviderThreadId>,
}

fn str_of<'a>(data: &'a Value, key: &str) -> Option<&'a str> {
    data.get(key).and_then(Value::as_str)
}

fn strip_provider_state(value: &mut Value) {
    match value {
        Value::Object(object) => {
            object.retain(|key, _| !PROVIDER_STATE_KEYS.contains(&key.as_str()));
            object.values_mut().for_each(strip_provider_state);
        }
        Value::Array(items) => items.iter_mut().for_each(strip_provider_state),
        _ => {}
    }
}

fn stripped(value: Option<&Value>) -> Option<Value> {
    let mut value = value.filter(|value| !value.is_null()).cloned()?;
    strip_provider_state(&mut value);
    Some(value)
}

/// A completed tool's content blocks as the activity normalizer reads them.
fn tool_output(data: &Value, failed: bool) -> Option<Value> {
    if failed {
        let error = stripped(data.get("error"))?;
        return Some(error.get("message").cloned().unwrap_or(error));
    }
    let content = stripped(data.get("content"))?;
    let Some(blocks) = content.as_array() else {
        return Some(content);
    };
    let text = blocks
        .iter()
        .filter_map(|block| {
            (block.get("type").and_then(Value::as_str) == Some("text"))
                .then(|| block.get("text").and_then(Value::as_str))
                .flatten()
        })
        .collect::<Vec<_>>()
        .join("\n");
    Some(Value::String(text))
}

/// Context-window occupancy of one step. OpenCode reports reasoning tokens
/// apart from output, so leaving them out under-reports thinking models.
fn step_usage(tokens: &Value, window: Option<u64>, now: Timestamp) -> Option<TokenUsage> {
    let count = |pointer: &str| {
        tokens
            .pointer(pointer)
            .and_then(Value::as_f64)
            .filter(|count| count.is_finite() && *count > 0.0)
            .map_or(0, |count| count as u64)
    };
    let input = count("/input");
    let output = count("/output");
    let reasoning = count("/reasoning");
    let cache_read = count("/cache/read");
    let cache_write = count("/cache/write");
    let used = input + output + reasoning + cache_read + cache_write;
    (used > 0).then_some(TokenUsage {
        used,
        max: window,
        input: Some(input + cache_read + cache_write),
        cached_input: Some(cache_read),
        output: Some(output),
        reasoning_output: Some(reasoning),
        prompt_cache_ttl_ms: None,
        updated_at: now,
    })
}

/// The failure an execution reported. OpenCode types quota and rate-limit
/// failures; it gives no structured reset time, so `reset_at` stays unset.
pub(super) fn execution_failure(error: Option<&Value>, fallback: Option<&str>) -> ProviderFailure {
    let kind = error
        .and_then(|error| str_of(error, "type"))
        .unwrap_or_default();
    let message = error
        .and_then(|error| str_of(error, "message").or_else(|| error.as_str()))
        .or(fallback)
        .unwrap_or("OpenCode reported an error.");
    let class = match kind {
        "provider.quota" | "provider.rate-limit" => FailureClass::UsageLimit,
        kind if kind.starts_with("provider.") => FailureClass::ProviderError,
        kind if kind.starts_with("permission") => FailureClass::PermissionError,
        _ => FailureClass::Unknown,
    };
    let mut failure = ProviderFailure::new(class, message);
    if !kind.is_empty() {
        failure = failure.with_code(kind);
    }
    failure.retryable = Some(class == FailureClass::UsageLimit);
    failure
}

/// The request kind a permission action asks for.
fn approval_kind(action: &str) -> RuntimeRequestKind {
    match action {
        "bash" | "shell" | "command" => RuntimeRequestKind::CommandApproval,
        "edit" | "write" | "multiedit" => RuntimeRequestKind::FileChangeApproval,
        "patch" | "apply_patch" => RuntimeRequestKind::ApplyPatchApproval,
        "read" | "list" | "glob" | "grep" => RuntimeRequestKind::FileReadApproval,
        _ => RuntimeRequestKind::Permission,
    }
}

/// Waku's access mode, applied locally: what reaches Waku is whatever the
/// agent's own rules mark `ask`; the mode only decides who answers.
fn auto_replies(mode: RuntimeMode, action: &str) -> bool {
    match mode {
        RuntimeMode::AutoAcceptEdits => matches!(action, "edit" | "write" | "patch"),
        RuntimeMode::Auto | RuntimeMode::FullAccess => true,
        _ => false,
    }
}

/// One form field per question, keyed by the field's own `key`.
pub(super) fn form_questions(form: &FormInfo) -> Vec<UserInputQuestion> {
    form.fields
        .iter()
        .filter_map(|field| {
            let (key, title, options, multi_select, note) = match field {
                FormField::String {
                    key,
                    title,
                    options,
                    ..
                } => (key, title.clone(), options.clone(), false, None),
                FormField::Number { key, title, .. }
                | FormField::Integer { key, title, .. }
                | FormField::Boolean { key, title, .. } => (key, title.clone(), None, false, None),
                FormField::Multiselect {
                    key,
                    title,
                    options,
                    ..
                } => (key, title.clone(), Some(options.clone()), true, None),
                FormField::External {
                    key, title, url, ..
                } => (key, title.clone(), None, false, Some(url.clone())),
                FormField::Unknown => return None,
            };
            let header = title
                .filter(|title| !title.trim().is_empty())
                .unwrap_or_else(|| key.clone());
            let question = note.unwrap_or_else(|| {
                let title = form.title.trim();
                if title.is_empty() {
                    header.clone()
                } else {
                    title.to_owned()
                }
            });
            Some(UserInputQuestion {
                id: key.clone(),
                header,
                question,
                options: options
                    .into_iter()
                    .flatten()
                    .map(|option| UserInputOption {
                        label: option.label,
                        description: option.description,
                    })
                    .collect(),
                multi_select,
            })
        })
        .collect()
}

impl Normalizer {
    pub(super) fn new(
        ids: Arc<dyn IdAllocator>,
        clock: Arc<dyn Clock>,
        provider_session_id: ProviderSessionId,
        mode: RuntimeMode,
    ) -> Self {
        Self {
            coalescer: DeltaCoalescer::new(clock.clone()),
            ids,
            clock,
            provider_session_id,
            mode,
            turn: None,
            step_message: None,
            step_model: None,
            model: None,
            context_windows: HashMap::new(),
            permissions: OpenCodePermissionState::default(),
            pending: HashMap::new(),
            stranded: Vec::new(),
            stop_pending: false,
            parts: HashMap::new(),
            tools: HashMap::new(),
            shells: HashMap::new(),
            provider_thread: None,
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

    pub(super) fn has_pending_snapshots(&mut self) -> bool {
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

    fn context_window(&self) -> Option<u64> {
        self.step_model
            .as_ref()
            .or(self.model.as_ref())
            .and_then(|model| {
                self.context_windows
                    .get(&format!("{}/{}", model.provider_id, model.id))
                    .copied()
            })
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
            native_turn_ref: (!turn.reference.native.id.is_empty())
                .then(|| turn.reference.native.to_provider_ref(DRIVER)),
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

    /// Starts tracking an accepted turn and announces it.
    pub(super) fn begin_turn(&mut self, turn: OpenTurn) -> Vec<AdapterEvent> {
        let snapshot = self.turn_snapshot(&turn, ProviderTurnStatus::Running);
        self.turn = Some(turn);
        self.stop_pending = false;
        let mut events = self.offer(vec![AdapterEvent::ProviderTurnUpdated { turn: snapshot }]);
        // Requests a previous session left pending belong to this run now.
        for request in std::mem::take(&mut self.stranded) {
            let (more, _) = if request.get("fields").is_some() {
                (self.form(&request), Vec::new())
            } else {
                self.permission(&request)
            };
            events.extend(more);
        }
        events
    }

    /// Ends the open turn with its one terminal.
    pub(super) fn finish_turn(
        &mut self,
        outcome: TurnOutcome,
        disposition: ThreadDisposition,
    ) -> Vec<AdapterEvent> {
        let Some(turn) = self.turn.take() else {
            return Vec::new();
        };
        let mut events = self.coalescer.flush();
        let status = match &outcome {
            TurnOutcome::Completed => ProviderTurnStatus::Completed,
            TurnOutcome::Interrupted => ProviderTurnStatus::Interrupted,
            TurnOutcome::Cancelled => ProviderTurnStatus::Cancelled,
            TurnOutcome::Failed { .. } => ProviderTurnStatus::Failed,
        };
        events.push(AdapterEvent::ProviderTurnUpdated {
            turn: self.turn_snapshot(&turn, status),
        });
        events.push(AdapterEvent::TurnTerminal {
            terminal: TurnTerminal {
                run: turn.run,
                provider_turn: Some(turn.reference.clone()),
                outcome,
                thread_disposition: disposition,
                child_turns_continue: false,
                background_pending: false,
                summary: None,
            },
        });
        self.parts.clear();
        self.tools.clear();
        self.step_message = None;
        self.step_model = None;
        self.permissions.pending.clear();
        self.permissions.responding.clear();
        self.pending.clear();
        events
    }

    /// One envelope of this session. `(events, effects)`.
    pub(super) fn on_event(
        &mut self,
        kind: &str,
        data: &Value,
    ) -> (Vec<AdapterEvent>, Vec<Effect>) {
        let mut effects = Vec::new();
        let events = match kind {
            "session.execution.started" => {
                if self.stop_pending && self.turn.is_none() {
                    effects.push(Effect::Interrupt);
                }
                Vec::new()
            }
            "session.step.started" => {
                self.step_message = str_of(data, "assistantMessageID").map(str::to_owned);
                self.step_model =
                    serde_json::from_value(data.get("model").cloned().unwrap_or(Value::Null)).ok();
                Vec::new()
            }
            "session.step.ended" => self.usage(data),
            "session.step.failed" => {
                if let Some(turn) = self.turn.as_mut() {
                    turn.step_error = data
                        .pointer("/error/message")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                }
                self.usage(data)
            }
            "session.execution.succeeded" => {
                let outcome = match &self.turn {
                    Some(turn) if turn.interrupted => TurnOutcome::Interrupted,
                    _ => TurnOutcome::Completed,
                };
                self.finish_turn(outcome, ThreadDisposition::Reusable)
            }
            "session.execution.failed" => {
                let fallback = self.turn.as_ref().and_then(|turn| turn.step_error.clone());
                let outcome = match &self.turn {
                    Some(turn) if turn.interrupted => TurnOutcome::Interrupted,
                    _ => TurnOutcome::Failed {
                        failure: execution_failure(data.get("error"), fallback.as_deref()),
                    },
                };
                self.finish_turn(outcome, ThreadDisposition::Reusable)
            }
            "session.execution.interrupted" => {
                // A shutdown keeps the run's claim; the next boot resumes it.
                if str_of(data, "reason") == Some("shutdown") {
                    Vec::new()
                } else {
                    self.finish_turn(TurnOutcome::Interrupted, ThreadDisposition::Reusable)
                }
            }
            "session.text.started" => self.start_part(data, false),
            "session.reasoning.started" => self.start_part(data, true),
            "session.text.delta" => self.delta(data, false),
            "session.reasoning.delta" => self.delta(data, true),
            "session.text.ended" => self.end_part(data, false),
            "session.reasoning.ended" => self.end_part(data, true),
            "session.tool.input.started" => self.tool_started(data),
            "session.tool.called" => self.tool_called(data),
            "session.tool.progress" => self.tool_progress(data),
            "session.tool.success" => self.tool_finished(data, false),
            "session.tool.failed" => self.tool_finished(data, true),
            "permission.asked" => {
                let (events, more) = self.permission(data);
                effects.extend(more);
                events
            }
            "permission.replied" | "permission.rejected" => {
                let id = str_of(data, "requestID").or_else(|| str_of(data, "id"));
                match id {
                    Some(id) => self.resolve(id, RequestStatus::Resolved),
                    None => Vec::new(),
                }
            }
            "form.created" => match data.get("form") {
                Some(form) => self.form(form),
                None => Vec::new(),
            },
            "form.replied" => match str_of(data, "id") {
                Some(id) => self.resolve(id, RequestStatus::Resolved),
                None => Vec::new(),
            },
            "form.cancelled" => match str_of(data, "id") {
                Some(id) => self.resolve(id, RequestStatus::Cancelled),
                None => Vec::new(),
            },
            "session.model.selected" => {
                if let Ok(model) =
                    serde_json::from_value(data.get("model").cloned().unwrap_or(Value::Null))
                {
                    self.model = Some(model);
                }
                Vec::new()
            }
            "session.shell.started" => self.shell(data, true),
            "session.shell.ended" => self.shell(data, false),
            "session.compaction.started" => self.compaction(None, ItemStatus::Running),
            "session.compaction.ended" => {
                self.compaction(str_of(data, "text"), ItemStatus::Completed)
            }
            "session.compaction.failed" => self.compaction(None, ItemStatus::Failed),
            "session.retry.scheduled" => self.retry(data),
            "session.deleted" => {
                // Another client deleted the native session: the thread
                // cannot take another turn.
                self.finish_turn(
                    TurnOutcome::Failed {
                        failure: ProviderFailure::new(
                            FailureClass::ProviderError,
                            "The OpenCode session was deleted.",
                        )
                        .with_code("session_deleted"),
                    },
                    ThreadDisposition::Broken,
                )
            }
            _ => Vec::new(),
        };
        (events, effects)
    }

    fn usage(&mut self, data: &Value) -> Vec<AdapterEvent> {
        let window = self.context_window();
        let now = self.now();
        let Some(usage) = data
            .get("tokens")
            .and_then(|tokens| step_usage(tokens, window, now))
        else {
            return Vec::new();
        };
        let Some(turn) = self.turn.as_mut() else {
            return Vec::new();
        };
        turn.usage = Some(usage);
        let turn = self.turn.as_ref().expect("checked above");
        let snapshot = self.turn_snapshot(turn, ProviderTurnStatus::Running);
        self.offer(vec![AdapterEvent::ProviderTurnUpdated { turn: snapshot }])
    }

    fn part_key(&self, data: &Value) -> Option<String> {
        let message = str_of(data, "assistantMessageID")
            .map(str::to_owned)
            .or_else(|| self.step_message.clone())?;
        let ordinal = data.get("ordinal").and_then(Value::as_u64).unwrap_or(0);
        Some(format!("{message}:{ordinal}"))
    }

    fn start_part(&mut self, data: &Value, reasoning: bool) -> Vec<AdapterEvent> {
        if let Some(key) = self.part_key(data) {
            let started_at = self.now();
            self.parts.entry(key).or_insert(Part {
                text: String::new(),
                reasoning,
                started_at,
            });
        }
        Vec::new()
    }

    fn delta(&mut self, data: &Value, reasoning: bool) -> Vec<AdapterEvent> {
        let Some(delta) = str_of(data, "delta").filter(|delta| !delta.is_empty()) else {
            return Vec::new();
        };
        let Some(key) = self.part_key(data) else {
            return Vec::new();
        };
        let now = self.now();
        let part = self.parts.entry(key.clone()).or_insert(Part {
            text: String::new(),
            reasoning,
            started_at: now,
        });
        part.text.push_str(delta);
        let events = self.part_snapshot(&key, true);
        self.offer(events)
    }

    fn end_part(&mut self, data: &Value, reasoning: bool) -> Vec<AdapterEvent> {
        let Some(key) = self.part_key(data) else {
            return Vec::new();
        };
        let now = self.now();
        let part = self.parts.entry(key.clone()).or_insert(Part {
            text: String::new(),
            reasoning,
            started_at: now,
        });
        // The server's authoritative copy; the deltas already carried it.
        if let Some(text) = str_of(data, "text") {
            part.text = text.to_owned();
        }
        let events = self.part_snapshot(&key, false);
        self.offer(events)
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
            item.native_item_ref = Some(NativeRef::strong(key).to_provider_ref(DRIVER));
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
        item.native_item_ref = Some(NativeRef::strong(key).to_provider_ref(DRIVER));
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

    fn tool_snapshot(
        &self,
        id: &str,
        output: Option<&Value>,
        status: ItemStatus,
        exit_code: Option<i32>,
    ) -> Vec<AdapterEvent> {
        let (Some(turn), Some(tool)) = (self.turn.as_ref(), self.tools.get(id)) else {
            return Vec::new();
        };
        let now = self.now();
        let pt = turn.provider_thread_id;
        let item_id: TurnItemId = self.bind(pt, EntityKind::Item, id);
        let node_id: NodeId = self.bind(pt, EntityKind::Node, id);
        let failed = status == ItemStatus::Failed;
        let normalized = tool_item(
            &tool.name,
            None,
            tool.input.as_ref(),
            output,
            failed,
            exit_code,
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

    fn tool_started(&mut self, data: &Value) -> Vec<AdapterEvent> {
        let Some(id) = str_of(data, "id") else {
            return Vec::new();
        };
        let started_at = self.now();
        self.tools.entry(id.to_owned()).or_insert(Tool {
            name: str_of(data, "name").unwrap_or("tool").to_owned(),
            input: None,
            started_at,
        });
        let events = self.tool_snapshot(id, None, ItemStatus::Running, None);
        self.offer(events)
    }

    fn tool_called(&mut self, data: &Value) -> Vec<AdapterEvent> {
        let Some(id) = str_of(data, "id") else {
            return Vec::new();
        };
        let started_at = self.now();
        let tool = self.tools.entry(id.to_owned()).or_insert(Tool {
            name: str_of(data, "name").unwrap_or("tool").to_owned(),
            input: None,
            started_at,
        });
        if let Some(input) = stripped(data.get("input")) {
            tool.input = Some(input);
        }
        let events = self.tool_snapshot(id, None, ItemStatus::Running, None);
        self.offer(events)
    }

    fn tool_progress(&mut self, data: &Value) -> Vec<AdapterEvent> {
        let Some(id) = str_of(data, "id") else {
            return Vec::new();
        };
        let (Some(turn), Some(tool)) = (self.turn.as_ref(), self.tools.get(id)) else {
            return Vec::new();
        };
        // A running command's output so far, as the bash tool reports it.
        let Some(output) = data
            .pointer("/metadata/output")
            .and_then(Value::as_str)
            .filter(|_| {
                waku_protocol::model::ActivityKind::from_tool_name(&tool.name)
                    == waku_protocol::model::ActivityKind::Command
            })
        else {
            return Vec::new();
        };
        let item: TurnItemId = self.bind(turn.provider_thread_id, EntityKind::Item, id);
        vec![AdapterEvent::CommandOutputDelta {
            item,
            chunk: output.as_bytes().to_vec(),
            replace: true,
        }]
    }

    fn tool_finished(&mut self, data: &Value, failed: bool) -> Vec<AdapterEvent> {
        let Some(id) = str_of(data, "id") else {
            return Vec::new();
        };
        if !self.tools.contains_key(id) {
            return Vec::new();
        }
        let output = tool_output(data, failed);
        let exit_code = data
            .pointer("/metadata/exit")
            .and_then(Value::as_i64)
            .and_then(|code| i32::try_from(code).ok());
        let status = if failed {
            ItemStatus::Failed
        } else {
            ItemStatus::Completed
        };
        let events = self.tool_snapshot(id, output.as_ref(), status, exit_code);
        self.tools.remove(id);
        self.offer(events)
    }

    fn request_snapshot(
        &self,
        turn: &OpenTurn,
        native: &str,
        kind: RuntimeRequestKind,
        options: Vec<PermissionOption>,
        questions: Vec<UserInputQuestion>,
    ) -> RuntimeRequest {
        RuntimeRequest {
            id: self.bind(turn.provider_thread_id, EntityKind::Request, native),
            thread_id: turn.run.thread_id,
            node_id: turn.run.root_node_id,
            provider_turn_id: Some(turn.reference.provider_turn_id),
            native_request_ref: Some(ProviderRef::strong(DRIVER, native)),
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

    /// `permission.asked` (or a pending permission re-listed on resume).
    pub(super) fn permission(&mut self, data: &Value) -> (Vec<AdapterEvent>, Vec<Effect>) {
        let Some(id) = str_of(data, "id") else {
            return (Vec::new(), Vec::new());
        };
        if self.pending.contains_key(id) || self.permissions.responding.contains(id) {
            return (Vec::new(), Vec::new());
        }
        let strings = |name: &str| {
            data.get(name)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect::<Vec<_>>()
        };
        let request = OpenCodePermissionRequest {
            permission: str_of(data, "action").unwrap_or_default().to_owned(),
            patterns: strings("resources"),
            always: strings("save"),
        };
        if auto_replies(self.mode, &request.permission) || self.permissions.is_approved(&request) {
            self.permissions.responding.insert(id.to_owned());
            return (
                Vec::new(),
                vec![Effect::AutoReply {
                    request: id.to_owned(),
                }],
            );
        }
        let Some(turn) = self.turn.as_ref() else {
            // No run to show it in: keep it for the next turn.
            self.stranded.push(data.clone());
            return (Vec::new(), Vec::new());
        };
        let kind = approval_kind(&request.permission);
        let options = vec![
            PermissionOption {
                id: OPTION_ONCE.to_owned(),
                label: "Allow once".to_owned(),
                allow: true,
            },
            PermissionOption {
                id: OPTION_ALWAYS.to_owned(),
                label: "Always allow".to_owned(),
                allow: true,
            },
            PermissionOption {
                id: OPTION_REJECT.to_owned(),
                label: "Deny".to_owned(),
                allow: false,
            },
        ];
        let runtime_request = self.request_snapshot(turn, id, kind, options.clone(), Vec::new());
        let prompt = if request.patterns.is_empty() {
            str_of(data, "message")
                .map(str::trim)
                .filter(|message| !message.is_empty())
                .map(str::to_owned)
                .or_else(|| (!request.permission.is_empty()).then(|| request.permission.clone()))
        } else {
            Some(request.patterns.join(", "))
        };
        let now = self.now();
        let mut item = Self::item(
            turn,
            waku_orchestrator::provider::derived::request_item(runtime_request.id),
            now,
            ItemStatus::Waiting,
            TurnItemKind::ApprovalRequest {
                request_id: runtime_request.id,
                request_kind: kind,
                prompt,
                app_name: None,
                options,
            },
        );
        item.title = (!request.permission.is_empty()).then(|| request.permission.clone());
        self.permissions.pending.insert(id.to_owned(), request);
        self.pending.insert(
            id.to_owned(),
            Pending {
                request: runtime_request.clone(),
                fields: None,
            },
        );
        let events = self.offer(vec![
            AdapterEvent::TurnItemUpdated { item },
            AdapterEvent::RuntimeRequestUpdated {
                request: runtime_request,
            },
        ]);
        (events, Vec::new())
    }

    /// `form.created` (or a pending form re-listed on resume). Never
    /// auto-cancelled: the agent is blocked on it.
    pub(super) fn form(&mut self, data: &Value) -> Vec<AdapterEvent> {
        let Ok(form) = serde_json::from_value::<FormInfo>(data.clone()) else {
            return Vec::new();
        };
        if self.pending.contains_key(&form.id) {
            return Vec::new();
        }
        let questions = form_questions(&form);
        if questions.is_empty() {
            return Vec::new();
        }
        let Some(turn) = self.turn.as_ref() else {
            self.stranded.push(data.clone());
            return Vec::new();
        };
        let request = self.request_snapshot(
            turn,
            &form.id,
            RuntimeRequestKind::UserInput,
            Vec::new(),
            questions,
        );
        self.pending.insert(
            form.id.clone(),
            Pending {
                request: request.clone(),
                fields: Some(form.fields),
            },
        );
        self.offer(vec![AdapterEvent::RuntimeRequestUpdated { request }])
    }

    /// A request answered (here or by another client) or withdrawn.
    pub(super) fn resolve(&mut self, native: &str, status: RequestStatus) -> Vec<AdapterEvent> {
        self.permissions.pending.remove(native);
        self.permissions.responding.remove(native);
        let Some(pending) = self.pending.remove(native) else {
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

    /// Session-level shells outlive the turn that started them.
    fn shell(&mut self, data: &Value, running: bool) -> Vec<AdapterEvent> {
        let shell = data.get("shell").unwrap_or(&Value::Null);
        let Some(id) = str_of(shell, "id") else {
            return Vec::new();
        };
        let Some(provider_thread_id) = self
            .turn
            .as_ref()
            .map(|turn| turn.provider_thread_id)
            .or(self.provider_thread)
        else {
            return Vec::new();
        };
        if running {
            self.shells.insert(id.to_owned(), true);
        } else {
            self.shells.remove(id);
        }
        // Not through the coalescer's run scope: the work outlives the turn.
        let mut events = self.coalescer.flush();
        events.push(AdapterEvent::BackgroundWorkUpdated {
            work: BackgroundWorkItem {
                provider_thread_id,
                task: PendingBackgroundTask {
                    kind: PendingTaskKind::Command,
                    task_id: id.to_owned(),
                    description: str_of(shell, "command").map(str::to_owned),
                    parent_task_id: None,
                    child_thread_id: None,
                    item_id: None,
                },
                live: running,
            },
        });
        events
    }

    fn compaction(&mut self, summary: Option<&str>, status: ItemStatus) -> Vec<AdapterEvent> {
        let Some(turn) = self.turn.as_ref() else {
            return Vec::new();
        };
        let now = self.now();
        let id: TurnItemId = self.bind(
            turn.provider_thread_id,
            EntityKind::Item,
            &format!("compaction:{}", turn.reference.ordinal),
        );
        let item = Self::item(
            turn,
            id,
            now,
            status,
            TurnItemKind::Compaction {
                driver: Some(DRIVER),
                summary: summary.map(str::to_owned),
                before_tokens: None,
                after_tokens: None,
            },
        );
        self.offer(vec![AdapterEvent::TurnItemUpdated { item }])
    }

    fn retry(&mut self, data: &Value) -> Vec<AdapterEvent> {
        let Some(turn) = self.turn.as_ref() else {
            return Vec::new();
        };
        let message = data
            .pointer("/action/message")
            .or_else(|| data.get("message"))
            .or_else(|| data.pointer("/error/message"))
            .and_then(Value::as_str)
            .unwrap_or("OpenCode is retrying the request.");
        let now = self.now();
        let id: TurnItemId = self.bind(
            turn.provider_thread_id,
            EntityKind::Item,
            &format!("retry:{}", turn.reference.ordinal),
        );
        let item = Self::item(
            turn,
            id,
            now,
            ItemStatus::Completed,
            TurnItemKind::SystemNotice {
                message: message.to_owned(),
                lifecycle: None,
            },
        );
        self.offer(vec![AdapterEvent::TurnItemUpdated { item }])
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn quota_and_rate_limits_are_usage_limits() {
        let failure = execution_failure(
            Some(&json!({"type": "provider.quota", "message": "Quota exceeded"})),
            None,
        );
        assert_eq!(failure.class, FailureClass::UsageLimit);
        assert_eq!(failure.reset_at, None);
        assert_eq!(
            execution_failure(
                Some(&json!({"type": "provider.auth", "message": "no key"})),
                None
            )
            .class,
            FailureClass::ProviderError
        );
        assert_eq!(
            execution_failure(Some(&json!({"type": "unknown"})), Some("step failed")).message,
            "step failed"
        );
    }

    fn normalizer_with_turn() -> Normalizer {
        let ids: Arc<dyn IdAllocator> = Arc::new(crate::driver::v2::SeqIdAllocator::new());
        let clock: Arc<dyn Clock> = Arc::new(waku_orchestrator::ManualClock::new(0));
        let mut norm = Normalizer::new(
            ids.clone(),
            clock,
            ProviderSessionId(ids.fresh(EntityKind::Session)),
            RuntimeMode::Ask,
        );
        let pt = ProviderThreadId(ids.fresh(EntityKind::ProviderThread));
        norm.begin_turn(OpenTurn {
            run: RunRef {
                thread_id: ThreadId(ids.fresh(EntityKind::Node)),
                run_id: RunId(ids.fresh(EntityKind::Node)),
                attempt_id: RunAttemptId(ids.fresh(EntityKind::Node)),
                root_node_id: NodeId(ids.fresh(EntityKind::Node)),
            },
            reference: ProviderTurnRef {
                provider_turn_id: ProviderTurnId(ids.fresh(EntityKind::ProviderTurn)),
                native: NativeRef::weak("msg_1"),
                ordinal: 1,
            },
            provider_thread_id: pt,
            started_at: 0,
            interrupted: false,
            usage: None,
            step_error: None,
        });
        norm
    }

    fn outcomes(events: &[AdapterEvent]) -> Vec<(TurnOutcome, ThreadDisposition)> {
        events
            .iter()
            .filter_map(|event| match event {
                AdapterEvent::TurnTerminal { terminal } => {
                    Some((terminal.outcome.clone(), terminal.thread_disposition))
                }
                _ => None,
            })
            .collect()
    }

    #[test]
    fn exactly_one_terminal_per_turn() {
        let mut norm = normalizer_with_turn();
        let (events, _) = norm.on_event("session.execution.succeeded", &json!({}));
        assert_eq!(
            outcomes(&events),
            [(TurnOutcome::Completed, ThreadDisposition::Reusable)]
        );
        // A second outcome (a steer that joined the execution) settles nothing.
        let (events, _) = norm.on_event("session.execution.failed", &json!({}));
        assert!(outcomes(&events).is_empty());
        // `session.idle` is never a terminal.
        let mut norm = normalizer_with_turn();
        let (events, _) = norm.on_event("session.idle", &json!({}));
        assert!(outcomes(&events).is_empty());
        assert!(norm.turn.is_some());
    }

    #[test]
    fn a_shutdown_interrupt_keeps_the_turn_and_a_user_one_ends_it() {
        let mut norm = normalizer_with_turn();
        let (events, _) = norm.on_event(
            "session.execution.interrupted",
            &json!({"reason": "shutdown"}),
        );
        assert!(outcomes(&events).is_empty());
        let (events, _) =
            norm.on_event("session.execution.interrupted", &json!({"reason": "user"}));
        assert_eq!(
            outcomes(&events),
            [(TurnOutcome::Interrupted, ThreadDisposition::Reusable)]
        );
    }

    #[test]
    fn a_deleted_session_breaks_the_thread() {
        let mut norm = normalizer_with_turn();
        let (events, _) = norm.on_event("session.deleted", &json!({}));
        let outcomes = outcomes(&events);
        assert!(matches!(
            &outcomes[..],
            [(TurnOutcome::Failed { .. }, ThreadDisposition::Broken)]
        ));
    }

    #[test]
    fn full_access_answers_permissions_itself() {
        let mut norm = normalizer_with_turn();
        norm.mode = RuntimeMode::FullAccess;
        let (events, effects) = norm.on_event(
            "permission.asked",
            &json!({"id": "per_1", "action": "bash", "resources": ["ls"]}),
        );
        assert!(events.is_empty());
        assert_eq!(
            effects,
            [Effect::AutoReply {
                request: "per_1".to_owned()
            }]
        );
        norm.mode = RuntimeMode::Ask;
        let (events, effects) = norm.on_event(
            "permission.asked",
            &json!({"id": "per_2", "action": "edit", "resources": ["a.rs"]}),
        );
        assert!(effects.is_empty());
        assert!(events.iter().any(|event| matches!(
            event,
            AdapterEvent::RuntimeRequestUpdated { request } if request.kind == RuntimeRequestKind::FileChangeApproval
        )));
    }

    #[test]
    fn provider_state_never_reaches_a_transcript() {
        let mut value =
            json!({"command": "ls", "providerState": {"x": 1}, "nested": [{"resultState": 2}]});
        strip_provider_state(&mut value);
        assert_eq!(value, json!({"command": "ls", "nested": [{}]}));
    }
}

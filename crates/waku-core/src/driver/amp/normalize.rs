//! Amp stream-json frames → normalized `AdapterEvent`s (RFC 0002 "Amp").
//!
//! Amp writes whole assistant messages, not deltas: each `assistant` frame
//! is one message (its text and thinking blocks) plus the tool calls it
//! starts, and `user` frames carry the tool results. The turn ends at the
//! assistant's `stop_reason: "end_turn"`, or at an error `result`.
//!
//! Identity: turns bind by ordinal (Amp has no turn id); messages by the
//! turn ordinal and their position (weak); tool calls by Amp's `tool_use`
//! id. Frames of a subagent (`parent_tool_use_id` set) never reach the
//! parent's transcript.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::Value;
use waku_protocol::orchestration::*;

use super::protocol;
use crate::driver::claude::protocol::{context_tokens, tool_result_text};
use crate::driver::v2::coalescer::DeltaCoalescer;
use crate::driver::v2::{
    AdapterEvent, Clock, EntityKind, IdAllocator, NativeRef, ProviderThreadRef, ProviderTurnRef,
    RunRef, ThreadDisposition, TurnOutcome, TurnTerminal,
};

const DRIVER: DriverKind = DriverKind::Amp;

#[derive(Default, Debug)]
pub struct Output {
    pub events: Vec<AdapterEvent>,
    /// The open turn ended (its terminal is in `events`).
    pub turn_ended: bool,
}

/// Fixed per session.
pub struct Scope {
    pub ids: Arc<dyn IdAllocator>,
    pub clock: Arc<dyn Clock>,
}

pub struct OpenTurn {
    pub run: RunRef,
    pub reference: ProviderTurnRef,
    pub provider_thread_id: ProviderThreadId,
    pub started_at: Timestamp,
    pub interrupt_requested: bool,
    tokens: Option<u64>,
    messages: u32,
}

struct ToolState {
    native: String,
    name: String,
    input: Value,
    item_id: TurnItemId,
    node_id: NodeId,
    done: bool,
}

pub struct Normalizer {
    scope: Scope,
    pub turn: Option<OpenTurn>,
    tools: BTreeMap<String, ToolState>,
    coalescer: DeltaCoalescer,
}

fn str_of<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
}

fn is_subagent(frame: &Value) -> bool {
    frame
        .get("parent_tool_use_id")
        .is_some_and(|parent| !parent.is_null())
}

impl Normalizer {
    pub fn new(scope: Scope) -> Self {
        let coalescer = DeltaCoalescer::new(scope.clock.clone());
        Self {
            scope,
            turn: None,
            tools: BTreeMap::new(),
            coalescer,
        }
    }

    fn now(&self) -> Timestamp {
        self.scope.clock.now()
    }

    fn bind<T: From<uuid::Uuid>>(&self, scope: uuid::Uuid, kind: EntityKind, native: &str) -> T {
        T::from(self.scope.ids.bind(scope, kind, native))
    }

    fn offer(&mut self, events: Vec<AdapterEvent>) -> Vec<AdapterEvent> {
        events
            .into_iter()
            .flat_map(|event| self.coalescer.offer(event))
            .collect()
    }

    /// Registers the turn `start_turn` writes, and reports it running: Amp
    /// takes a written message at once (there is no echo to wait for).
    pub fn begin_turn(
        &mut self,
        run: RunRef,
        thread: &ProviderThreadRef,
        ordinal: u32,
    ) -> (ProviderTurnRef, Vec<AdapterEvent>) {
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
        let turn = OpenTurn {
            run,
            reference: reference.clone(),
            provider_thread_id,
            started_at: self.now(),
            interrupt_requested: false,
            tokens: None,
            messages: 0,
        };
        let snapshot = self.turn_snapshot(&turn, ProviderTurnStatus::Running);
        self.turn = Some(turn);
        let events = self.offer(vec![AdapterEvent::ProviderTurnUpdated { turn: snapshot }]);
        (reference, events)
    }

    /// Drops a turn whose message never reached Amp (no terminal).
    pub fn abandon_turn(&mut self, attempt: RunAttemptId) {
        if self
            .turn
            .as_ref()
            .is_some_and(|turn| turn.run.attempt_id == attempt)
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
                max: None,
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
        match frame.get("type").and_then(Value::as_str)? {
            "assistant" if is_subagent(frame) => {}
            "user" if is_subagent(frame) => {}
            "assistant" => self.assistant(frame, &mut out),
            "user" => self.user(frame, &mut out),
            "result" => {
                if self.turn.is_none() {
                    return Some(out);
                }
                let interrupted = self
                    .turn
                    .as_ref()
                    .is_some_and(|turn| turn.interrupt_requested);
                let is_error = frame.get("is_error").and_then(Value::as_bool) == Some(true);
                let (outcome, summary) = if interrupted {
                    (TurnOutcome::Interrupted, None)
                } else if is_error {
                    let failure = protocol::result_failure(frame);
                    let summary = Some(failure.message.clone());
                    (TurnOutcome::Failed { failure }, summary)
                } else {
                    (TurnOutcome::Completed, None)
                };
                self.finish_turn(outcome, summary, &mut out);
            }
            // `system/init` names the thread Waku already knows; other
            // system frames are diagnostics.
            "system" => return None,
            _ => return None,
        }
        Some(out)
    }

    fn assistant(&mut self, frame: &Value, out: &mut Output) {
        let Some(message) = frame.get("message") else {
            return;
        };
        if self.turn.is_none() {
            return;
        }
        if let Some(tokens) = message.get("usage").and_then(context_tokens) {
            let changed = self.turn.as_mut().is_some_and(|turn| {
                let changed = turn.tokens != Some(tokens);
                turn.tokens = Some(tokens);
                changed
            });
            if changed && let Some(turn) = self.turn.as_ref() {
                let turn = self.turn_snapshot(turn, ProviderTurnStatus::Running);
                let events = self.offer(vec![AdapterEvent::ProviderTurnUpdated { turn }]);
                out.events.extend(events);
            }
        }
        let mut text = String::new();
        let mut reasoning = String::new();
        let mut tools = Vec::new();
        for block in message
            .get("content")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            match block.get("type").and_then(Value::as_str) {
                Some("text") => text.push_str(str_of(block, "text").unwrap_or_default()),
                Some("thinking") => {
                    reasoning.push_str(str_of(block, "thinking").unwrap_or_default());
                }
                Some("tool_use") => tools.push(block.clone()),
                // Redacted thinking is provider-private control data.
                _ => {}
            }
        }
        if !text.is_empty() || !reasoning.is_empty() {
            self.message(&text, &reasoning, out);
        }
        for block in tools {
            self.tool_started(&block, out);
        }
        // The turn is over when the assistant stops for its own reasons
        // rather than to call a tool.
        if message.get("stop_reason").and_then(Value::as_str) == Some("end_turn") {
            let interrupted = self
                .turn
                .as_ref()
                .is_some_and(|turn| turn.interrupt_requested);
            let outcome = if interrupted {
                TurnOutcome::Interrupted
            } else {
                TurnOutcome::Completed
            };
            self.finish_turn(outcome, None, out);
        }
    }

    /// One complete assistant message: its reasoning and text items.
    fn message(&mut self, text: &str, reasoning: &str, out: &mut Output) {
        let Some(turn) = self.turn.as_mut() else {
            return;
        };
        turn.messages += 1;
        let key = format!("turn-{}:message-{}", turn.reference.ordinal, turn.messages);
        let Some(turn) = self.turn.as_ref() else {
            return;
        };
        let pt = turn.provider_thread_id.0;
        let mut events = Vec::new();
        if !reasoning.is_empty() {
            let id: TurnItemId = self.bind(pt, EntityKind::Item, &format!("{key}#thinking"));
            if let Some(item) = self.item(
                id,
                ItemStatus::Completed,
                TurnItemKind::Reasoning {
                    text: reasoning.to_owned(),
                    streaming: false,
                },
            ) {
                events.push(AdapterEvent::TurnItemUpdated { item });
            }
        }
        if !text.is_empty() {
            let id: TurnItemId = self.bind(pt, EntityKind::Item, &key);
            let message_id: MessageId = self.bind(pt, EntityKind::Message, &key);
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
                    text: text.to_owned(),
                    display_text: None,
                    attachments: Vec::new(),
                    context: Vec::new(),
                    notification: None,
                    sender_thread_id: None,
                    streaming: false,
                    created_at: turn.started_at,
                    updated_at: now,
                },
            });
            if let Some(mut item) = self.item(
                id,
                ItemStatus::Completed,
                TurnItemKind::AssistantMessage {
                    message_id,
                    text: text.to_owned(),
                    streaming: false,
                },
            ) {
                item.native_item_ref = Some(NativeRef::weak(&key).to_provider_ref(DRIVER));
                events.push(AdapterEvent::TurnItemUpdated { item });
            }
        }
        let events = self.offer(events);
        out.events.extend(events);
    }

    fn tool_started(&mut self, block: &Value, out: &mut Output) {
        let Some(native) = str_of(block, "id").map(str::to_owned) else {
            return;
        };
        if self.tools.contains_key(&native) {
            return;
        }
        let Some(pt) = self.turn.as_ref().map(|turn| turn.provider_thread_id.0) else {
            return;
        };
        let tool = ToolState {
            native: native.clone(),
            name: str_of(block, "name").unwrap_or("tool").to_owned(),
            input: block.get("input").cloned().unwrap_or(Value::Null),
            item_id: self.bind(pt, EntityKind::Item, &native),
            node_id: self.bind(pt, EntityKind::Node, &native),
            done: false,
        };
        self.tools.insert(native.clone(), tool);
        if let Some((item, node)) = self.tool_rows(&native, ItemStatus::Running, None, false) {
            let events = self.offer(vec![
                AdapterEvent::NodeUpdated { node },
                AdapterEvent::TurnItemUpdated { item },
            ]);
            out.events.extend(events);
        }
    }

    fn user(&mut self, frame: &Value, out: &mut Output) {
        for block in frame
            .pointer("/message/content")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            if block.get("type").and_then(Value::as_str) != Some("tool_result") {
                continue;
            }
            let Some(native) = str_of(&block, "tool_use_id").map(str::to_owned) else {
                continue;
            };
            let Some(tool) = self.tools.get_mut(&native) else {
                continue;
            };
            if tool.done {
                continue;
            }
            tool.done = true;
            let failed = block.get("is_error").and_then(Value::as_bool) == Some(true);
            let output = tool_result_text(block.get("content"));
            let status = if failed {
                ItemStatus::Failed
            } else {
                ItemStatus::Completed
            };
            if let Some((item, node)) = self.tool_rows(&native, status, output, failed) {
                let events = self.offer(vec![
                    AdapterEvent::NodeUpdated { node },
                    AdapterEvent::TurnItemUpdated { item },
                ]);
                out.events.extend(events);
            }
        }
    }

    fn tool_rows(
        &self,
        native: &str,
        status: ItemStatus,
        output: Option<String>,
        failed: bool,
    ) -> Option<(TurnItem, ExecutionNode)> {
        let tool = self.tools.get(native)?;
        let turn = self.turn.as_ref()?;
        let input = &tool.input;
        let kind = match tool.name.as_str() {
            "Bash" | "bash" => TurnItemKind::CommandExecution {
                input: str_of(input, "cmd")
                    .or_else(|| str_of(input, "command"))
                    .unwrap_or_default()
                    .to_owned(),
                output,
                output_truncated: false,
                output_omitted: false,
                output_indicates_failure: failed,
                exit_code: None,
            },
            "edit_file" | "create_file" | "Edit" | "Write" => TurnItemKind::FileChange {
                file_name: str_of(input, "path")
                    .or_else(|| str_of(input, "file_path"))
                    .unwrap_or_default()
                    .to_owned(),
                additions: None,
                deletions: None,
                diff: None,
                old_text: str_of(input, "old_str").map(str::to_owned),
                new_text: str_of(input, "new_str")
                    .or_else(|| str_of(input, "content"))
                    .map(str::to_owned),
                changes: Vec::new(),
                body_omitted: false,
            },
            "Grep" | "glob" | "Glob" => TurnItemKind::FileSearch {
                pattern: str_of(input, "pattern")
                    .or_else(|| str_of(input, "filePattern"))
                    .map(str::to_owned),
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
        item.node_id = Some(tool.node_id);
        item.native_item_ref = Some(NativeRef::weak(&tool.native).to_provider_ref(DRIVER));
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

    /// Ends the open turn with `outcome`: closes its open tools, then the
    /// provider turn and exactly one terminal. The thread stays reusable:
    /// even an interrupt that ended the process leaves the thread to
    /// `threads continue`.
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
        let open = self
            .tools
            .values()
            .filter(|tool| !tool.done)
            .map(|tool| tool.native.clone())
            .collect::<Vec<_>>();
        for native in open {
            if let Some(tool) = self.tools.get_mut(&native) {
                tool.done = true;
            }
            if let Some((item, node)) = self.tool_rows(&native, item_status, None, false) {
                let events = self.offer(vec![
                    AdapterEvent::NodeUpdated { node },
                    AdapterEvent::TurnItemUpdated { item },
                ]);
                out.events.extend(events);
            }
        }
        let Some(turn) = self.turn.take() else {
            return;
        };
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
        self.tools.clear();
        out.turn_ended = true;
    }

    /// Everything still held by the coalescer.
    pub fn flush(&mut self) -> Vec<AdapterEvent> {
        self.coalescer.flush()
    }
}

#[cfg(test)]
mod tests;

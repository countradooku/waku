//! Claude stream-json frames → normalized `AdapterEvent`s (RFC 0002
//! "Claude", the `DriverEvent` → `AdapterEvent` table).
//!
//! [`Normalizer`] is the session's stream state: the open turn, streaming
//! messages, tool calls, pending requests, subagents and background tasks.
//! It is pure — frames in, events and replies out — so every path is unit
//! tested without a process. Native ids bind through the session's
//! `IdAllocator`: the provider thread scopes items (`msg_…` message ids and
//! `toolu_…` tool ids, strong), requests (`can_use_tool` request ids, strong)
//! and subagents (the launching tool id). Turns have no native id at start,
//! so they bind by ordinal and carry the turn-ending assistant uuid as a weak
//! native ref (RFC 0002 note 6).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use serde_json::Value;
use waku_orchestrator::provider::derived;
use waku_protocol::orchestration::*;

use super::protocol::{self, ALLOW, DENY, LimitEvidence, ResultOutcome};
use crate::driver::v2::coalescer::DeltaCoalescer;
use crate::driver::v2::{
    AdapterEvent, Clock, EntityKind, IdAllocator, NativeRef, ProviderThreadRef, ProviderTurnRef,
    RunRef, RuntimeReply, ThreadDisposition, TurnOutcome, TurnTerminal,
};

const DRIVER: DriverKind = DriverKind::Claude;

/// What a normalized frame asks the session to do.
#[derive(Default, Debug)]
pub struct Output {
    pub events: Vec<AdapterEvent>,
    /// Frames to write back (automatic approvals).
    pub writes: Vec<Value>,
    /// The open turn's prompt was echoed: `start_turn` resolves.
    pub echoed: bool,
    /// A `control_response`: the request id and its payload or error text.
    pub control: Option<(String, Result<Value, String>)>,
    /// The open turn ended (its terminal is in `events`).
    pub turn_ended: bool,
}

/// Fixed per session.
pub struct Scope {
    pub session_id: ProviderSessionId,
    pub thread_id: ThreadId,
    pub instance: ProviderInstanceId,
    pub ids: Arc<dyn IdAllocator>,
    pub clock: Arc<dyn Clock>,
}

pub struct OpenTurn {
    pub run: RunRef,
    pub reference: ProviderTurnRef,
    pub provider_thread_id: ProviderThreadId,
    pub started_at: Timestamp,
    /// The CLI echoed the prompt (or produced output for a wake).
    pub echoed: bool,
    pub interrupt_requested: bool,
    tokens: Option<u64>,
    window: Option<u64>,
}

#[derive(Default)]
struct MessageState {
    native: String,
    text: String,
    reasoning: String,
    text_streamed: bool,
    reasoning_streamed: bool,
    done: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ToolRole {
    /// A transcript tool row (item plus node).
    Item,
    /// Represented by a plan, a request or a subagent instead.
    Other,
    Subagent,
}

struct ToolState {
    name: String,
    input: Value,
    item_id: TurnItemId,
    node_id: NodeId,
    role: ToolRole,
    done: bool,
}

struct SubagentState {
    node_id: NodeId,
    child_thread: ProviderThreadId,
    child_turn: ProviderTurnRef,
    run: RunRef,
    prompt: String,
    title: Option<String>,
    model: Option<String>,
    task_id: Option<String>,
    progress: Option<String>,
    backgrounded: bool,
    status: SubagentStatus,
    started_at: Timestamp,
}

struct PendingRequest {
    request: RuntimeRequest,
    input: Value,
    question: bool,
}

pub struct Normalizer {
    scope: Scope,
    /// The provider thread the running process serves.
    pub thread: Option<ProviderThreadRef>,
    pub turn: Option<OpenTurn>,
    /// The last turn's run and ordinal: a wake continues it.
    last: Option<(RunRef, ProviderThreadId, u32)>,
    auto_approve: bool,
    seq: u64,
    messages: BTreeMap<u64, MessageState>,
    message_index: HashMap<String, u64>,
    current_message: Option<u64>,
    tools: BTreeMap<u64, ToolState>,
    tool_index: HashMap<String, u64>,
    requests: HashMap<String, PendingRequest>,
    subagents: BTreeMap<String, SubagentState>,
    task_tools: HashMap<String, String>,
    task_kinds: HashMap<String, PendingTaskKind>,
    task_descriptions: HashMap<String, String>,
    live_tasks: BTreeSet<String>,
    limit: LimitEvidence,
    last_model: Option<String>,
    /// Uuid of the last root assistant message: the turn's weak native id
    /// and the thread's resume point.
    pub last_assistant_uuid: Option<String>,
    coalescer: DeltaCoalescer,
}

fn str_of<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
}

fn is_agent_tool(name: &str) -> bool {
    name.eq_ignore_ascii_case("task") || name.eq_ignore_ascii_case("agent")
}

fn subagent_status(status: &str) -> SubagentStatus {
    match status {
        "completed" | "complete" | "success" | "succeeded" => SubagentStatus::Completed,
        "failed" | "error" | "errored" => SubagentStatus::Failed,
        "stopped" | "killed" | "cancelled" | "canceled" => SubagentStatus::Cancelled,
        _ => SubagentStatus::Running,
    }
}

impl Normalizer {
    pub fn new(scope: Scope, auto_approve: bool) -> Self {
        let coalescer = DeltaCoalescer::new(scope.clock.clone());
        Self {
            scope,
            thread: None,
            turn: None,
            last: None,
            auto_approve,
            seq: 0,
            messages: BTreeMap::new(),
            message_index: HashMap::new(),
            current_message: None,
            tools: BTreeMap::new(),
            tool_index: HashMap::new(),
            requests: HashMap::new(),
            subagents: BTreeMap::new(),
            task_tools: HashMap::new(),
            task_kinds: HashMap::new(),
            task_descriptions: HashMap::new(),
            live_tasks: BTreeSet::new(),
            limit: LimitEvidence::default(),
            last_model: None,
            last_assistant_uuid: None,
            coalescer,
        }
    }

    pub fn set_auto_approve(&mut self, auto_approve: bool) {
        self.auto_approve = auto_approve;
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

    /// Detached work the CLI will wake the session for.
    pub fn has_live_tasks(&self) -> bool {
        !self.live_tasks.is_empty()
    }

    /// Registers the turn `start_turn` is about to write.
    pub fn begin_turn(
        &mut self,
        run: RunRef,
        thread: &ProviderThreadRef,
        ordinal: u32,
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
                strength: RefStrength::Weak,
            },
            ordinal,
        };
        self.limit = LimitEvidence::default();
        self.turn = Some(OpenTurn {
            run,
            reference: reference.clone(),
            provider_thread_id,
            started_at: self.now(),
            echoed: false,
            interrupt_requested: false,
            tokens: None,
            window: None,
        });
        reference
    }

    /// Drops a turn whose prompt never reached the CLI (no terminal: only an
    /// accepted turn gets one).
    pub fn abandon_turn(&mut self, attempt: RunAttemptId) {
        if self
            .turn
            .as_ref()
            .is_some_and(|turn| turn.run.attempt_id == attempt && !turn.echoed)
        {
            self.turn = None;
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
            native_turn_ref: (!turn.reference.native.id.is_empty())
                .then(|| turn.reference.native.to_provider_ref(DRIVER)),
            ordinal: turn.reference.ordinal,
            status,
            started_at: Some(turn.started_at),
            completed_at: terminal.then_some(now),
            token_usage: turn.tokens.map(|used| TokenUsage {
                used,
                max: turn.window,
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

    fn offer(&mut self, events: Vec<AdapterEvent>) -> Vec<AdapterEvent> {
        events
            .into_iter()
            .flat_map(|event| self.coalescer.offer(event))
            .collect()
    }

    /// Model output with no open turn: the CLI woke itself for detached
    /// work. The wake continues the last turn's run as a new provider turn.
    fn ensure_turn(&mut self, out: &mut Output) -> bool {
        if let Some(turn) = self.turn.as_mut() {
            // Model output before the echo: the prompt was taken.
            if !turn.echoed {
                turn.echoed = true;
                let snapshot = self
                    .turn
                    .as_ref()
                    .map(|turn| self.turn_snapshot(turn, ProviderTurnStatus::Running));
                if let Some(turn) = snapshot {
                    out.events.push(AdapterEvent::ProviderTurnUpdated { turn });
                }
                out.echoed = true;
            }
            return true;
        }
        let Some((run, provider_thread_id, ordinal)) = self.last else {
            return false;
        };
        let ordinal = ordinal + 1;
        let thread = ProviderThreadRef {
            provider_thread_id,
            native: NativeRef::strong(""),
            cursor: None,
        };
        self.begin_turn(run, &thread, ordinal);
        if let Some(turn) = self.turn.as_mut() {
            turn.echoed = true;
        }
        let snapshot = self
            .turn
            .as_ref()
            .map(|turn| self.turn_snapshot(turn, ProviderTurnStatus::Running));
        if let Some(turn) = snapshot {
            out.events.push(AdapterEvent::ProviderTurnUpdated { turn });
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
        match frame.get("type").and_then(Value::as_str)? {
            "system" => self.system(frame, &mut out)?,
            "control_request" => self.control_request(frame, &mut out)?,
            "control_response" => {
                let response = frame.get("response")?;
                let request_id = str_of(response, "request_id")
                    .or_else(|| str_of(frame, "request_id"))?
                    .to_owned();
                let result = if response.get("subtype").and_then(Value::as_str) == Some("error") {
                    Err(str_of(response, "error")
                        .unwrap_or("the control request failed")
                        .to_owned())
                } else {
                    Ok(response.get("response").cloned().unwrap_or(Value::Null))
                };
                out.control = Some((request_id, result));
            }
            "stream_event" => {
                if frame
                    .get("parent_tool_use_id")
                    .is_some_and(|parent| !parent.is_null())
                {
                    return Some(out);
                }
                self.stream_event(frame, &mut out)?;
            }
            "assistant" => {
                if frame
                    .get("parent_tool_use_id")
                    .is_some_and(|parent| !parent.is_null())
                {
                    return Some(out);
                }
                self.assistant(frame, &mut out);
            }
            "user" => {
                if frame
                    .get("parent_tool_use_id")
                    .is_some_and(|parent| !parent.is_null())
                {
                    return Some(out);
                }
                self.user(frame, &mut out);
            }
            "result" => self.result(frame, &mut out),
            "rate_limit_event" => {
                if let Some(limit) = protocol::rate_limit(frame) {
                    if limit.blocked {
                        self.limit.rate_limit = Some(limit);
                    } else if self
                        .limit
                        .rate_limit
                        .as_ref()
                        .is_some_and(|known| known.limit_type == limit.limit_type)
                    {
                        self.limit.rate_limit = None;
                    }
                }
            }
            _ => return None,
        }
        Some(out)
    }

    // -- system ------------------------------------------------------------------

    fn system(&mut self, frame: &Value, out: &mut Output) -> Option<()> {
        match frame.get("subtype").and_then(Value::as_str)? {
            "init" => {
                if let Some(model) = str_of(frame, "model") {
                    self.last_model = Some(model.to_owned());
                }
            }
            "task_started" => self.task_started(frame, out),
            "task_progress" => self.task_progress(frame, out),
            "task_updated" => {
                let task_id = str_of(frame, "task_id")?.to_owned();
                let patch = frame.get("patch").cloned().unwrap_or(Value::Null);
                if patch.get("is_backgrounded").and_then(Value::as_bool) == Some(true) {
                    if let Some(tool) = self.task_tools.get(&task_id).cloned()
                        && let Some(subagent) = self.subagents.get_mut(&tool)
                    {
                        subagent.backgrounded = true;
                    }
                    self.set_live(&task_id, true, out);
                }
                if let Some(status) = str_of(&patch, "status")
                    && subagent_status(status).is_terminal()
                {
                    self.task_settled(&task_id, status, None, out);
                }
            }
            "task_notification" => {
                let task_id = str_of(frame, "task_id")?.to_owned();
                let status = str_of(frame, "status").unwrap_or("completed");
                let summary = str_of(frame, "summary").map(str::to_owned);
                self.task_settled(&task_id, status, summary, out);
            }
            "background_tasks_changed" => {
                let tasks = frame
                    .get("tasks")
                    .or_else(|| frame.get("background_tasks"))
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let mut level = BTreeSet::new();
                for entry in &tasks {
                    if entry.get("ambient").and_then(Value::as_bool) == Some(true) {
                        continue;
                    }
                    let Some(task_id) = entry.as_str().or_else(|| str_of(entry, "task_id")) else {
                        continue;
                    };
                    if !self.task_kinds.contains_key(task_id) {
                        let kind = task_kind(entry, None);
                        self.task_kinds.insert(task_id.to_owned(), kind);
                    }
                    if let Some(description) = str_of(entry, "description") {
                        self.task_descriptions
                            .entry(task_id.to_owned())
                            .or_insert_with(|| description.to_owned());
                    }
                    level.insert(task_id.to_owned());
                }
                let gone = self
                    .live_tasks
                    .difference(&level)
                    .cloned()
                    .collect::<Vec<_>>();
                for task_id in gone {
                    self.set_live(&task_id, false, out);
                }
                for task_id in level {
                    self.set_live(&task_id, true, out);
                }
            }
            _ => return None,
        }
        Some(())
    }

    fn set_live(&mut self, task_id: &str, live: bool, out: &mut Output) {
        let changed = if live {
            self.live_tasks.insert(task_id.to_owned())
        } else {
            self.live_tasks.remove(task_id)
        };
        if !changed {
            return;
        }
        let Some(provider_thread_id) = self.thread.as_ref().map(|thread| thread.provider_thread_id)
        else {
            return;
        };
        let work = waku_orchestrator::provider::BackgroundWorkItem {
            provider_thread_id,
            task: PendingBackgroundTask {
                kind: self
                    .task_kinds
                    .get(task_id)
                    .cloned()
                    .unwrap_or(PendingTaskKind::Command),
                task_id: task_id.to_owned(),
                description: self.task_descriptions.get(task_id).cloned(),
                parent_task_id: None,
                child_thread_id: None,
            },
            live,
        };
        let events = self.offer(vec![AdapterEvent::BackgroundWorkUpdated { work }]);
        out.events.extend(events);
    }

    fn task_started(&mut self, frame: &Value, out: &mut Output) {
        if frame.get("ambient").and_then(Value::as_bool) == Some(true) {
            return;
        }
        let Some(task_id) = str_of(frame, "task_id").map(str::to_owned) else {
            return;
        };
        let tool_use_id = str_of(frame, "tool_use_id").map(str::to_owned);
        let tool_name = tool_use_id
            .as_ref()
            .and_then(|id| self.tool_index.get(id))
            .and_then(|seq| self.tools.get(seq))
            .map(|tool| tool.name.clone());
        let kind = task_kind(frame, tool_name.as_deref());
        self.task_kinds.insert(task_id.clone(), kind);
        if let Some(description) = str_of(frame, "description") {
            self.task_descriptions
                .insert(task_id.clone(), description.to_owned());
        }
        let backgrounded = frame.get("is_backgrounded").and_then(Value::as_bool) == Some(true);
        if kind == PendingTaskKind::Subagent {
            let key = tool_use_id.clone().unwrap_or_else(|| task_id.clone());
            if !self.subagents.contains_key(&key) {
                let prompt = str_of(frame, "prompt")
                    .or_else(|| str_of(frame, "description"))
                    .unwrap_or_default()
                    .to_owned();
                let title = str_of(frame, "description").map(str::to_owned);
                if !self.open_subagent(&key, prompt, title, None, out) {
                    return;
                }
            }
            self.task_tools.insert(task_id.clone(), key.clone());
            if let Some(subagent) = self.subagents.get_mut(&key) {
                subagent.task_id = Some(task_id.clone());
                subagent.backgrounded |= backgrounded;
                if let Some(description) = str_of(frame, "description") {
                    subagent.title = Some(description.to_owned());
                }
            }
            self.emit_subagent(&key, out);
        }
        if backgrounded {
            self.set_live(&task_id, true, out);
        }
    }

    fn task_progress(&mut self, frame: &Value, out: &mut Output) {
        let Some(task_id) = str_of(frame, "task_id") else {
            return;
        };
        let Some(key) = self.task_tools.get(task_id).cloned() else {
            return;
        };
        let workflow = frame
            .get("workflow_progress")
            .and_then(Value::as_array)
            .and_then(|entries| entries.last())
            .and_then(|entry| str_of(entry, "title").or_else(|| str_of(entry, "label")));
        let line = str_of(frame, "summary")
            .or(workflow)
            .or_else(|| str_of(frame, "description"))
            .filter(|line| self.task_descriptions.get(task_id).map(String::as_str) != Some(*line))
            .map(one_line);
        if let Some(subagent) = self.subagents.get_mut(&key) {
            if line.is_none() || subagent.progress == line {
                return;
            }
            subagent.progress = line;
        }
        self.emit_subagent(&key, out);
    }

    fn task_settled(
        &mut self,
        task_id: &str,
        status: &str,
        summary: Option<String>,
        out: &mut Output,
    ) {
        if let Some(key) = self.task_tools.get(task_id).cloned() {
            self.finish_subagent(&key, subagent_status(status), summary, out);
        }
        self.set_live(task_id, false, out);
    }

    // -- subagents -----------------------------------------------------------------

    /// A subagent keyed by its launching tool id, with its own provider
    /// thread and one child turn.
    fn open_subagent(
        &mut self,
        key: &str,
        prompt: String,
        title: Option<String>,
        model: Option<String>,
        out: &mut Output,
    ) -> bool {
        let Some(turn) = self.turn.as_ref() else {
            return false;
        };
        let run = turn.run;
        let pt = turn.provider_thread_id;
        let node_id: NodeId = self.bind(pt.0, EntityKind::Subagent, key);
        let child_thread: ProviderThreadId = self.bind(pt.0, EntityKind::ProviderThread, key);
        let child_turn = ProviderTurnRef {
            provider_turn_id: self.bind(child_thread.0, EntityKind::ProviderTurn, key),
            native: NativeRef::weak(key),
            ordinal: 1,
        };
        let now = self.now();
        self.subagents.insert(
            key.to_owned(),
            SubagentState {
                node_id,
                child_thread,
                child_turn: child_turn.clone(),
                run,
                prompt,
                title,
                model,
                task_id: None,
                progress: None,
                backgrounded: false,
                status: SubagentStatus::Running,
                started_at: now,
            },
        );
        let events = vec![
            AdapterEvent::ProviderThreadUpdated {
                thread: ProviderThread {
                    id: child_thread,
                    driver: DRIVER,
                    provider_instance_id: self.scope.instance.clone(),
                    provider_session_id: Some(self.scope.session_id),
                    app_thread_id: Some(self.scope.thread_id),
                    owner_node_id: Some(node_id),
                    native_thread_ref: Some(NativeRef::weak(key).to_provider_ref(DRIVER)),
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
            },
            AdapterEvent::ProviderTurnUpdated {
                turn: ProviderTurn {
                    id: child_turn.provider_turn_id,
                    provider_thread_id: child_thread,
                    node_id,
                    run_attempt_id: None,
                    native_turn_ref: Some(child_turn.native.to_provider_ref(DRIVER)),
                    ordinal: 1,
                    status: ProviderTurnStatus::Running,
                    started_at: Some(now),
                    completed_at: None,
                    token_usage: None,
                    turn_token_usage: None,
                },
            },
        ];
        let events = self.offer(events);
        out.events.extend(events);
        true
    }

    fn subagent_row(&self, key: &str) -> Option<Subagent> {
        let state = self.subagents.get(key)?;
        let now = self.now();
        Some(Subagent {
            id: state.node_id,
            thread_id: state.run.thread_id,
            run_id: Some(state.run.run_id),
            parent_node_id: state.run.root_node_id,
            origin: SubagentOrigin::ProviderNative,
            created_by: Actor::Agent,
            driver: DRIVER,
            provider_instance_id: self.scope.instance.clone(),
            provider_thread_id: Some(state.child_thread),
            child_thread_id: None,
            native_task_ref: state
                .task_id
                .as_ref()
                .map(|task| ProviderRef::strong(DRIVER, task)),
            prompt: state.prompt.clone(),
            title: state.title.clone(),
            model: state.model.clone(),
            status: state.status,
            progress: state.progress.clone(),
            result: None,
            completion_wake: None,
            completion_delivery: None,
            started_at: Some(state.started_at),
            completed_at: state.status.is_terminal().then_some(now),
            updated_at: now,
        })
    }

    fn emit_subagent(&mut self, key: &str, out: &mut Output) {
        if let Some(subagent) = self.subagent_row(key) {
            let events = self.offer(vec![AdapterEvent::SubagentUpdated { subagent }]);
            out.events.extend(events);
        }
    }

    fn finish_subagent(
        &mut self,
        key: &str,
        status: SubagentStatus,
        result: Option<String>,
        out: &mut Output,
    ) {
        let Some(state) = self.subagents.get_mut(key) else {
            return;
        };
        if state.status.is_terminal() {
            return;
        }
        state.status = if status.is_terminal() {
            status
        } else {
            SubagentStatus::Completed
        };
        let status = state.status;
        let run = state.run;
        let child_turn = state.child_turn.clone();
        let child_thread = state.child_thread;
        let node_id = state.node_id;
        let started_at = state.started_at;
        let Some(mut subagent) = self.subagent_row(key) else {
            return;
        };
        subagent.result = result;
        let now = self.now();
        let (turn_status, outcome) = match status {
            SubagentStatus::Completed => (ProviderTurnStatus::Completed, TurnOutcome::Completed),
            SubagentStatus::Cancelled => (ProviderTurnStatus::Cancelled, TurnOutcome::Cancelled),
            _ => (
                ProviderTurnStatus::Failed,
                TurnOutcome::Failed {
                    failure: ProviderFailure::new(FailureClass::Unknown, "The subagent failed.")
                        .with_code("subagent_failed"),
                },
            ),
        };
        let events = vec![
            AdapterEvent::SubagentUpdated { subagent },
            AdapterEvent::ProviderTurnUpdated {
                turn: ProviderTurn {
                    id: child_turn.provider_turn_id,
                    provider_thread_id: child_thread,
                    node_id,
                    run_attempt_id: None,
                    native_turn_ref: Some(child_turn.native.to_provider_ref(DRIVER)),
                    ordinal: 1,
                    status: turn_status,
                    started_at: Some(started_at),
                    completed_at: Some(now),
                    token_usage: None,
                    turn_token_usage: None,
                },
            },
            AdapterEvent::TurnTerminal {
                terminal: TurnTerminal {
                    run,
                    provider_turn: Some(child_turn),
                    outcome,
                    thread_disposition: ThreadDisposition::Reusable,
                    child_turns_continue: false,
                    background_pending: false,
                    summary: None,
                },
            },
        ];
        let events = self.offer(events);
        out.events.extend(events);
    }

    // -- approvals and questions ----------------------------------------------------

    fn control_request(&mut self, frame: &Value, out: &mut Output) -> Option<()> {
        let request = frame.get("request")?;
        if request.get("subtype").and_then(Value::as_str) != Some("can_use_tool") {
            return None;
        }
        let native = str_of(frame, "request_id")?.to_owned();
        let tool = str_of(request, "tool_name").unwrap_or("tool").to_owned();
        let input = request.get("input").cloned().unwrap_or(Value::Null);
        let question = tool == "AskUserQuestion";
        if !question && self.auto_approve {
            out.writes
                .push(protocol::control_success(&native, protocol::allow()));
            return Some(());
        }
        if !self.ensure_turn(out) {
            out.writes.push(protocol::control_success(
                &native,
                protocol::deny("No turn is running."),
            ));
            return Some(());
        }
        let turn = self.turn.as_ref()?;
        let pt = turn.provider_thread_id;
        let request_id: RuntimeRequestId = self.bind(pt.0, EntityKind::Request, &native);
        let now = self.now();
        let (kind, options, questions) = if question {
            let questions = input
                .get("questions")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .enumerate()
                .filter_map(|(index, question)| {
                    let text = str_of(question, "question")?.trim().to_owned();
                    Some(UserInputQuestion {
                        // Claude resolves answers by the complete question text.
                        id: text.clone(),
                        header: str_of(question, "header")
                            .map(str::to_owned)
                            .unwrap_or_else(|| format!("Question {}", index + 1)),
                        question: text,
                        options: question
                            .get("options")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .filter_map(|option| {
                                Some(UserInputOption {
                                    label: str_of(option, "label")?.trim().to_owned(),
                                    description: str_of(option, "description").map(str::to_owned),
                                })
                            })
                            .collect(),
                        multi_select: question
                            .get("multiSelect")
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                    })
                })
                .collect::<Vec<_>>();
            (RuntimeRequestKind::UserInput, Vec::new(), questions)
        } else {
            let kind = match tool.as_str() {
                "Bash" => RuntimeRequestKind::CommandApproval,
                "Edit" | "MultiEdit" | "Write" | "NotebookEdit" => {
                    RuntimeRequestKind::FileChangeApproval
                }
                "Read" | "Glob" | "Grep" | "LS" => RuntimeRequestKind::FileReadApproval,
                _ => RuntimeRequestKind::Permission,
            };
            let options = vec![
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
            ];
            (kind, options, Vec::new())
        };
        let runtime_request = RuntimeRequest {
            id: request_id,
            thread_id: turn.run.thread_id,
            node_id: turn.run.root_node_id,
            provider_turn_id: Some(turn.reference.provider_turn_id),
            native_request_ref: Some(ProviderRef::strong(DRIVER, &native)),
            kind,
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
        if !question {
            let prompt = str_of(&input, "command")
                .or_else(|| str_of(&input, "file_path"))
                .or_else(|| str_of(request, "description"))
                .map(str::to_owned)
                .unwrap_or_else(|| format!("Run {tool}"));
            if let Some(item) = self.item(
                derived::request_item(request_id),
                ItemStatus::Waiting,
                TurnItemKind::ApprovalRequest {
                    request_id,
                    request_kind: kind,
                    prompt: Some(prompt),
                    app_name: None,
                    options: runtime_request.options.clone(),
                },
            ) {
                events.push(AdapterEvent::TurnItemUpdated { item });
            }
        }
        events.push(AdapterEvent::RuntimeRequestUpdated {
            request: runtime_request.clone(),
        });
        self.requests.insert(
            native,
            PendingRequest {
                request: runtime_request,
                input,
                question,
            },
        );
        let events = self.offer(events);
        out.events.extend(events);
        Some(())
    }

    /// The `control_response` for a reply, and the resolved request.
    pub fn respond(
        &mut self,
        native: &str,
        reply: &RuntimeReply,
    ) -> Option<(Value, Vec<AdapterEvent>)> {
        let pending = self.requests.remove(native)?;
        let response = match (reply, pending.question) {
            (RuntimeReply::Answers(answers), true) => protocol::question_answers(
                &pending.input,
                &answers
                    .iter()
                    .map(|answer| (answer.question_id.clone(), answer.answers.clone()))
                    .collect::<Vec<_>>(),
            ),
            (RuntimeReply::Decision { option_id }, false) if option_id == ALLOW => {
                protocol::allow()
            }
            (RuntimeReply::Decision { .. }, false) => {
                protocol::deny("The user denied this tool call.")
            }
            (_, true) => protocol::deny("The user dismissed the question."),
            (_, false) => protocol::deny("The user dismissed this tool call."),
        };
        let now = self.now();
        let request = RuntimeRequest {
            status: RequestStatus::Resolved,
            resolved_at: Some(now),
            ..pending.request
        };
        let events = self.offer(vec![AdapterEvent::RuntimeRequestUpdated { request }]);
        Some((protocol::control_success(native, response), events))
    }

    // -- streaming -----------------------------------------------------------------

    fn message_seq(&mut self, native: &str) -> u64 {
        if let Some(seq) = self.message_index.get(native) {
            return *seq;
        }
        let seq = self.next_seq();
        self.messages.insert(
            seq,
            MessageState {
                native: native.to_owned(),
                ..MessageState::default()
            },
        );
        self.message_index.insert(native.to_owned(), seq);
        seq
    }

    fn message_events(&self, seq: u64, status: ItemStatus) -> Vec<AdapterEvent> {
        let done = status.is_terminal();
        let Some(state) = self.messages.get(&seq) else {
            return Vec::new();
        };
        let Some(turn) = self.turn.as_ref() else {
            return Vec::new();
        };
        let pt = turn.provider_thread_id.0;
        let mut events = Vec::new();
        if !state.reasoning.is_empty() {
            let id: TurnItemId =
                self.bind(pt, EntityKind::Item, &format!("{}#thinking", state.native));
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
            let id: TurnItemId = self.bind(pt, EntityKind::Item, &state.native);
            let message_id: MessageId = self.bind(pt, EntityKind::Message, &state.native);
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
                item.native_item_ref = Some(ProviderRef::strong(DRIVER, &state.native));
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

    fn stream_event(&mut self, frame: &Value, out: &mut Output) -> Option<()> {
        let event = frame.get("event")?;
        let kind = event.get("type").and_then(Value::as_str)?;
        if !self.ensure_turn(out) {
            return Some(());
        }
        match kind {
            "message_start" => {
                let native = event
                    .pointer("/message/id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("message-{}", self.seq + 1));
                if let Some(previous) = self.current_message {
                    self.complete_message(previous, ItemStatus::Completed, out);
                }
                let seq = self.message_seq(&native);
                self.current_message = Some(seq);
            }
            "content_block_delta" => {
                let delta = event.get("delta")?;
                let seq = match self.current_message {
                    Some(seq) => seq,
                    None => {
                        let native = format!("message-{}", self.seq + 1);
                        let seq = self.message_seq(&native);
                        self.current_message = Some(seq);
                        seq
                    }
                };
                let state = self.messages.get_mut(&seq)?;
                match delta.get("type").and_then(Value::as_str)? {
                    "text_delta" => {
                        state.text.push_str(str_of(delta, "text")?);
                        state.text_streamed = true;
                    }
                    "thinking_delta" => {
                        state.reasoning.push_str(str_of(delta, "thinking")?);
                        state.reasoning_streamed = true;
                    }
                    _ => return Some(()),
                }
                let events = self.message_events(seq, ItemStatus::Running);
                let events = self.offer(events);
                out.events.extend(events);
            }
            "message_stop" => {
                if let Some(seq) = self.current_message.take() {
                    self.complete_message(seq, ItemStatus::Completed, out);
                }
            }
            _ => {}
        }
        Some(())
    }

    fn assistant(&mut self, frame: &Value, out: &mut Output) {
        if !self.ensure_turn(out) {
            return;
        }
        if let Some(uuid) = str_of(frame, "uuid") {
            self.last_assistant_uuid = Some(uuid.to_owned());
            if let Some(turn) = self.turn.as_mut() {
                turn.reference.native = NativeRef::weak(uuid);
            }
        }
        if str_of(frame, "error") == Some("rate_limit") {
            self.limit.assistant_rate_limited = true;
        }
        let message = frame.get("message").cloned().unwrap_or(Value::Null);
        if let Some(model) = str_of(&message, "model") {
            self.last_model = Some(model.to_owned());
        }
        if let Some(tokens) = message.get("usage").and_then(protocol::context_tokens) {
            let snapshot = self.turn.as_mut().and_then(|turn| {
                (turn.tokens != Some(tokens)).then(|| {
                    turn.tokens = Some(tokens);
                })
            });
            if snapshot.is_some()
                && let Some(turn) = self.turn.as_ref()
            {
                let turn = self.turn_snapshot(turn, ProviderTurnStatus::Running);
                let events = self.offer(vec![AdapterEvent::ProviderTurnUpdated { turn }]);
                out.events.extend(events);
            }
        }
        let native = str_of(&message, "id")
            .map(str::to_owned)
            .unwrap_or_else(|| format!("message-{}", self.seq + 1));
        let seq = self.message_seq(&native);
        let mut changed = false;
        for block in message
            .get("content")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            match block.get("type").and_then(Value::as_str) {
                Some("text") => {
                    if let Some(state) = self.messages.get_mut(&seq)
                        && !state.text_streamed
                        && let Some(text) = str_of(&block, "text")
                    {
                        if !state.text.is_empty() {
                            state.text.push_str("\n\n");
                        }
                        state.text.push_str(text);
                        changed = true;
                    }
                }
                Some("thinking") => {
                    if let Some(state) = self.messages.get_mut(&seq)
                        && !state.reasoning_streamed
                        && let Some(text) = str_of(&block, "thinking")
                    {
                        state.reasoning.push_str(text);
                        changed = true;
                    }
                }
                Some("tool_use") => self.tool_started(&block, out),
                _ => {}
            }
        }
        if changed && self.messages.get(&seq).is_some_and(|state| !state.done) {
            let events = self.message_events(seq, ItemStatus::Running);
            let events = self.offer(events);
            out.events.extend(events);
        }
    }

    // -- tools ---------------------------------------------------------------------

    fn tool_started(&mut self, block: &Value, out: &mut Output) {
        let Some(id) = str_of(block, "id").map(str::to_owned) else {
            return;
        };
        if self.tool_index.contains_key(&id) {
            return;
        }
        let Some(turn) = self.turn.as_ref() else {
            return;
        };
        let pt = turn.provider_thread_id.0;
        let ordinal = turn.reference.ordinal;
        let run = turn.run;
        let name = str_of(block, "name").unwrap_or("tool").to_owned();
        let input = block.get("input").cloned().unwrap_or(Value::Null);
        let item_id: TurnItemId = self.bind(pt, EntityKind::Item, &id);
        let node_id: NodeId = self.bind(pt, EntityKind::Node, &id);
        let role = match name.as_str() {
            "TodoWrite" | "ExitPlanMode" | "AskUserQuestion" => ToolRole::Other,
            name if is_agent_tool(name) => ToolRole::Subagent,
            _ => ToolRole::Item,
        };
        let seq = self.next_seq();
        self.tool_index.insert(id.clone(), seq);
        self.tools.insert(
            seq,
            ToolState {
                name: name.clone(),
                input: input.clone(),
                item_id,
                node_id,
                role,
                done: false,
            },
        );
        let mut events = Vec::new();
        match name.as_str() {
            "TodoWrite" => {
                let steps = input
                    .get("todos")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .enumerate()
                    .map(|(index, todo)| PlanStep {
                        id: str_of(todo, "id")
                            .map(str::to_owned)
                            .unwrap_or_else(|| (index + 1).to_string()),
                        text: str_of(todo, "content")
                            .or_else(|| str_of(todo, "activeForm"))
                            .unwrap_or_default()
                            .to_owned(),
                        status: match str_of(todo, "status") {
                            Some("completed") => PlanStepStatus::Completed,
                            Some("in_progress") => PlanStepStatus::Running,
                            _ => PlanStepStatus::Pending,
                        },
                    })
                    .collect();
                let plan_id: PlanId = self.bind(pt, EntityKind::Plan, &format!("todo:{ordinal}"));
                events.push(AdapterEvent::PlanUpdated {
                    plan: Plan {
                        id: plan_id,
                        thread_id: run.thread_id,
                        run_id: Some(run.run_id),
                        node_id: run.root_node_id,
                        status: PlanStatus::Active,
                        kind: PlanKind::TodoList {
                            steps,
                            explanation: None,
                        },
                    },
                });
            }
            "ExitPlanMode" => {
                if let Some(markdown) = str_of(&input, "plan") {
                    let plan_id: PlanId = self.bind(pt, EntityKind::Plan, &format!("plan:{id}"));
                    events.push(AdapterEvent::PlanUpdated {
                        plan: Plan {
                            id: plan_id,
                            thread_id: run.thread_id,
                            run_id: Some(run.run_id),
                            node_id: run.root_node_id,
                            status: PlanStatus::Active,
                            kind: PlanKind::ProposedPlan {
                                markdown: markdown.trim().to_owned(),
                            },
                        },
                    });
                }
            }
            "AskUserQuestion" => {}
            name if is_agent_tool(name) => {
                let prompt = str_of(&input, "prompt")
                    .or_else(|| str_of(&input, "description"))
                    .unwrap_or_default()
                    .to_owned();
                let title = str_of(&input, "description").map(str::to_owned);
                let model = str_of(&input, "model").map(str::to_owned);
                if self.open_subagent(&id, prompt, title, model, out) {
                    self.emit_subagent(&id, out);
                }
            }
            _ => {
                if let Some((item, node)) =
                    self.tool_rows(seq, &id, ItemStatus::Running, None, false)
                {
                    events.push(AdapterEvent::NodeUpdated { node });
                    events.push(AdapterEvent::TurnItemUpdated { item });
                }
            }
        }
        let events = self.offer(events);
        out.events.extend(events);
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
            "Bash" => TurnItemKind::CommandExecution {
                input: str_of(input, "command").unwrap_or_default().to_owned(),
                output,
                output_truncated: false,
                output_omitted: false,
                output_indicates_failure: failed,
                exit_code: None,
            },
            "Edit" | "MultiEdit" | "Write" | "NotebookEdit" => TurnItemKind::FileChange {
                file_name: str_of(input, "file_path")
                    .or_else(|| str_of(input, "notebook_path"))
                    .unwrap_or_default()
                    .to_owned(),
                additions: None,
                deletions: None,
                diff: None,
                old_text: str_of(input, "old_string").map(str::to_owned),
                new_text: str_of(input, "new_string")
                    .or_else(|| str_of(input, "content"))
                    .map(str::to_owned),
                changes: Vec::new(),
                body_omitted: false,
            },
            "Glob" | "Grep" => TurnItemKind::FileSearch {
                pattern: str_of(input, "pattern").map(str::to_owned),
                results: Vec::new(),
            },
            "WebSearch" => TurnItemKind::WebSearch {
                patterns: str_of(input, "query")
                    .map(str::to_owned)
                    .into_iter()
                    .collect(),
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
        item.native_item_ref = Some(ProviderRef::strong(DRIVER, native));
        item.title = str_of(input, "description")
            .map(str::to_owned)
            .or_else(|| Some(tool.name.clone()));
        let done = status.is_terminal();
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
            completed_at: done.then_some(now),
        };
        Some((item, node))
    }

    fn user(&mut self, frame: &Value, out: &mut Output) {
        if frame.get("isReplay").and_then(Value::as_bool) == Some(true) {
            // `--replay-user-messages` echoes Waku's own prompt (and later a
            // steer once the CLI absorbed it): the turn starts here.
            let started = self.turn.as_mut().filter(|turn| !turn.echoed).map(|turn| {
                turn.echoed = true;
            });
            if started.is_some()
                && let Some(turn) = self.turn.as_ref()
            {
                let turn = self.turn_snapshot(turn, ProviderTurnStatus::Running);
                let events = self.offer(vec![AdapterEvent::ProviderTurnUpdated { turn }]);
                out.events.extend(events);
                out.echoed = true;
            }
            return;
        }
        let content = frame
            .pointer("/message/content")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for block in content {
            if block.get("type").and_then(Value::as_str) != Some("tool_result") {
                continue;
            }
            let Some(id) = str_of(&block, "tool_use_id").map(str::to_owned) else {
                continue;
            };
            let Some(seq) = self.tool_index.get(&id).copied() else {
                continue;
            };
            let failed = block.get("is_error").and_then(Value::as_bool) == Some(true);
            let output = protocol::tool_result_text(block.get("content"));
            let raw = block.get("content").cloned().unwrap_or(Value::Null);
            let patch = frame
                .get("tool_use_result")
                .and_then(|result| result.get("structuredPatch"))
                .and_then(Value::as_array)
                .map(|hunks| render_patch(hunks));
            let Some(tool) = self.tools.get_mut(&seq) else {
                continue;
            };
            if tool.done {
                continue;
            }
            tool.done = true;
            match tool.role {
                ToolRole::Item => {
                    let status = if failed {
                        ItemStatus::Failed
                    } else {
                        ItemStatus::Completed
                    };
                    if let Some((mut item, node)) = self.tool_rows(seq, &id, status, output, failed)
                    {
                        match &mut item.kind {
                            TurnItemKind::FileChange { diff, .. } => *diff = patch,
                            TurnItemKind::DynamicTool { output, .. } if raw.is_array() => {
                                *output = Some(JsonText(raw.to_string()));
                            }
                            _ => {}
                        }
                        let events = self.offer(vec![
                            AdapterEvent::NodeUpdated { node },
                            AdapterEvent::TurnItemUpdated { item },
                        ]);
                        out.events.extend(events);
                    }
                }
                ToolRole::Subagent => {
                    // A backgrounded agent's tool result only says it runs
                    // in the background; its notification settles it.
                    let backgrounded = self
                        .subagents
                        .get(&id)
                        .is_some_and(|subagent| subagent.backgrounded);
                    if !backgrounded {
                        let status = if failed {
                            SubagentStatus::Failed
                        } else {
                            SubagentStatus::Completed
                        };
                        self.finish_subagent(&id, status, output, out);
                    }
                }
                ToolRole::Other => {}
            }
        }
    }

    // -- the turn's end ------------------------------------------------------------

    fn result(&mut self, frame: &Value, out: &mut Output) {
        let Some(mut turn) = self.turn.take() else {
            return;
        };
        if let Some(window) = protocol::context_window(frame, self.last_model.as_deref()) {
            turn.window = Some(window);
        }
        let outcome = match protocol::classify_result(frame, turn.interrupt_requested, &self.limit)
        {
            ResultOutcome::Completed => TurnOutcome::Completed,
            ResultOutcome::Interrupted => TurnOutcome::Interrupted,
            ResultOutcome::Failed(failure) => TurnOutcome::Failed { failure },
        };
        let summary = match &outcome {
            TurnOutcome::Failed { failure } => Some(failure.message.clone()),
            _ => None,
        };
        self.turn = Some(turn);
        self.finish_turn(outcome, summary, out);
    }

    /// Ends the open turn with `outcome`: closes its streaming messages and
    /// open tools, then the provider turn and exactly one terminal.
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
            .filter(|(_, tool)| !tool.done && tool.role == ToolRole::Item)
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
        let Some(turn) = self.turn.take() else {
            return;
        };
        let status = match &outcome {
            TurnOutcome::Completed => ProviderTurnStatus::Completed,
            TurnOutcome::Interrupted => ProviderTurnStatus::Interrupted,
            TurnOutcome::Cancelled => ProviderTurnStatus::Cancelled,
            TurnOutcome::Failed { .. } => ProviderTurnStatus::Failed,
        };
        let child_turns_continue = self
            .subagents
            .values()
            .any(|subagent| subagent.status == SubagentStatus::Running);
        let background_pending =
            matches!(outcome, TurnOutcome::Completed) && !self.live_tasks.is_empty();
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
                child_turns_continue,
                background_pending,
                summary,
            },
        });
        self.last = Some((turn.run, turn.provider_thread_id, turn.reference.ordinal));
        self.current_message = None;
        self.messages.clear();
        self.message_index.clear();
        self.requests.clear();
        out.turn_ended = true;
    }

    /// Everything still held by the coalescer.
    pub fn flush(&mut self) -> Vec<AdapterEvent> {
        self.coalescer.flush()
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

    /// The native task id a subagent stop addresses.
    pub fn subagent_task(&self, node_id: NodeId) -> Option<String> {
        self.subagents
            .values()
            .find(|subagent| subagent.node_id == node_id)
            .and_then(|subagent| subagent.task_id.clone())
    }

    /// Forgets the last turn so a wake cannot continue a run on another
    /// provider thread (after a rollback or fork re-pointed the session).
    pub fn reset_thread(&mut self, thread: ProviderThreadRef) {
        if self
            .thread
            .as_ref()
            .is_some_and(|known| known.provider_thread_id != thread.provider_thread_id)
        {
            self.last = None;
            self.last_assistant_uuid = None;
            self.live_tasks.clear();
        }
        self.thread = Some(thread);
    }
}

/// What kind of detached work a task is.
fn task_kind(value: &Value, tool_name: Option<&str>) -> PendingTaskKind {
    let task_type = str_of(value, "task_type")
        .or_else(|| str_of(value, "subagent_type"))
        .unwrap_or_default()
        .to_ascii_lowercase();
    if task_type == "monitor" || tool_name.is_some_and(|name| name.eq_ignore_ascii_case("monitor"))
    {
        PendingTaskKind::Monitor
    } else if task_type.contains("agent")
        || task_type.contains("workflow")
        || task_type.contains("teammate")
        || task_type == "remote"
        || str_of(value, "subagent_type").is_some()
        || tool_name.is_some_and(is_agent_tool)
    {
        PendingTaskKind::Subagent
    } else {
        PendingTaskKind::Command
    }
}

/// A unified diff body from Claude's `structuredPatch` hunks.
fn render_patch(hunks: &[Value]) -> String {
    let mut diff = String::new();
    for hunk in hunks {
        let number = |key: &str| hunk.get(key).and_then(Value::as_u64).unwrap_or(0);
        diff.push_str(&format!(
            "@@ -{},{} +{},{} @@\n",
            number("oldStart"),
            number("oldLines"),
            number("newStart"),
            number("newLines")
        ));
        for line in hunk
            .get("lines")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some(line) = line.as_str() {
                diff.push_str(line);
                diff.push('\n');
            }
        }
    }
    diff
}

fn one_line(text: &str) -> String {
    let line = text
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or_default();
    let mut short = line.chars().take(160).collect::<String>();
    if short.len() < line.len() {
        short.push('…');
    }
    short
}

#[cfg(test)]
mod tests;

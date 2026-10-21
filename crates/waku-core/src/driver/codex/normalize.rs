//! Native app-server frames → `AdapterEvent` entity snapshots.
//!
//! The read loop owns the order: a response, notification or server request
//! is applied to [`State`] under its lock and the resulting events are sent
//! after the lock is released, through the shared `DeltaCoalescer`. Unknown
//! frames go to the raw log and are otherwise ignored.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use futures_util::future::BoxFuture;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use waku_orchestrator::provider::derived;
use waku_protocol::orchestration::*;

use super::protocol::{self, Decision};
use super::{DRIVER, Shared};
use crate::driver::v2::coalescer::DeltaCoalescer;
use crate::driver::v2::{RuntimeResponse, *};

/// A reply the read loop sends for a server request it handles itself.
pub(super) enum ServerAction {
    Respond { id: Value, result: Value },
    Reject { id: Value, message: String },
}

/// The root provider thread this session drives.
pub(super) struct Root {
    pub native: String,
    pub id: ProviderThreadId,
}

struct Starting {
    run: RunRef,
    ordinal: u32,
    turn: Option<String>,
    cancelled: bool,
}

struct OpenTurn {
    /// `None` for a turn Codex started on its own (a `/goal` continuation)
    /// in orchestrator mode: it belongs to no run until #59 decides.
    run: Option<RunRef>,
    reference: ProviderTurnRef,
    provider_thread_id: ProviderThreadId,
    started_at: Timestamp,
    cancel: bool,
    usage: Option<TokenUsage>,
    turn_usage: Option<TokenUsage>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ItemKind {
    Assistant,
    Reasoning,
    Command,
    FileChange,
    Tool,
    WebSearch,
}

struct OpenItem {
    turn: String,
    kind: ItemKind,
    text: String,
    part: Option<(bool, u64)>,
    started_at: Timestamp,
    completed: bool,
    process_id: Option<String>,
    command: String,
    streamed: bool,
}

struct Child {
    native: String,
    provider_thread_id: ProviderThreadId,
    node_id: NodeId,
    parent: RunRef,
    prompt: String,
    title: Option<String>,
    model: Option<String>,
    status: SubagentStatus,
    result: Option<String>,
    started_at: Timestamp,
    turns: u32,
}

struct ChildTurn {
    child: String,
    reference: ProviderTurnRef,
    started_at: Timestamp,
}

struct PendingRequest {
    rpc_id: Value,
    method: String,
    params: Value,
    decisions: Vec<Decision>,
    request: RuntimeRequest,
    question_ids: Vec<String>,
    by_message: bool,
}

struct Terminal {
    item: String,
    command: String,
    provider_thread_id: ProviderThreadId,
    live: bool,
}

/// Citation markers (`\u{e200}cite\u{e202}ref\u{e201}`) become numbered
/// markdown links to the web-search results they name; private control
/// markers never reach the transcript.
#[derive(Default)]
struct Citations {
    urls: HashMap<String, String>,
    numbers: HashMap<String, usize>,
    buffers: HashMap<String, String>,
}

const CITATION_START: char = '\u{e200}';
const CITATION_END: char = '\u{e201}';
const CITATION_SEPARATOR: char = '\u{e202}';

impl Citations {
    fn reset(&mut self) {
        *self = Self::default();
    }

    fn capture(&mut self, item: &Value) {
        for result in item
            .get("results")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let reference = result
                .get("ref_id")
                .or_else(|| result.get("refId"))
                .and_then(Value::as_str)
                .filter(|reference| !reference.is_empty());
            let url = result
                .get("url")
                .and_then(Value::as_str)
                .filter(|url| !url.is_empty());
            if let (Some(reference), Some(url)) = (reference, url) {
                self.urls.insert(reference.to_owned(), url.to_owned());
            }
        }
    }

    /// Rewrites a delta; a marker split across deltas waits in the item's
    /// buffer.
    fn delta(&mut self, item: &str, delta: &str) -> String {
        let mut input = self.buffers.remove(item).unwrap_or_default();
        input.push_str(delta);
        let (output, rest) = self.rewrite(&input);
        if !rest.is_empty() {
            self.buffers.insert(item.to_owned(), rest);
        }
        output
    }

    /// Rewrites a whole text (an item's final form).
    fn full(&mut self, text: &str) -> String {
        let (mut output, rest) = self.rewrite(text);
        output.push_str(&rest.replace([CITATION_START, CITATION_END, CITATION_SEPARATOR], ""));
        output
    }

    fn rewrite(&mut self, input: &str) -> (String, String) {
        let mut output = String::with_capacity(input.len());
        let mut rest = input;
        loop {
            let Some(start) = rest.find(CITATION_START) else {
                output.push_str(rest);
                return (output, String::new());
            };
            output.push_str(&rest[..start]);
            let marker_start = start + CITATION_START.len_utf8();
            let Some(end) = rest[marker_start..].find(CITATION_END) else {
                return (output, rest[start..].to_owned());
            };
            let marker = &rest[marker_start..marker_start + end];
            let rendered = self.render(marker);
            if !rendered.is_empty() {
                if output.chars().last().is_some_and(|c| !c.is_whitespace()) {
                    output.push(' ');
                }
                output.push_str(&rendered);
            }
            rest = &rest[marker_start + end + CITATION_END.len_utf8()..];
        }
    }

    fn render(&mut self, marker: &str) -> String {
        let mut parts = marker.split(CITATION_SEPARATOR);
        if parts.next() != Some("cite") {
            return String::new();
        }
        let mut links = Vec::new();
        for reference in parts.filter(|part| !part.is_empty()) {
            let Some(url) = self.urls.get(reference).cloned() else {
                continue;
            };
            let next = self.numbers.len() + 1;
            let number = *self.numbers.entry(reference.to_owned()).or_insert(next);
            let destination = url
                .replace('\\', "\\\\")
                .replace('(', "\\(")
                .replace(')', "\\)");
            links.push(format!("[{number}]({destination})"));
        }
        links.join(" ")
    }
}

pub(super) struct State {
    pub root: Option<Root>,
    pub goal: Option<ThreadGoal>,
    pub coalescer: DeltaCoalescer,
    starting: Option<Starting>,
    turns: HashMap<String, OpenTurn>,
    /// Root turns in native order since the thread was bound, and whether
    /// that is the whole history (a thread this session started).
    order: Vec<String>,
    order_complete: bool,
    items: HashMap<String, OpenItem>,
    requests: HashMap<RuntimeRequestId, PendingRequest>,
    children: HashMap<String, Child>,
    child_turns: HashMap<String, ChildTurn>,
    terminals: BTreeMap<String, Terminal>,
    rate_limits: Option<Value>,
    failures: HashMap<String, Value>,
    citations: Citations,
    last_run: Option<RunRef>,
}

impl State {
    pub fn new(coalescer: DeltaCoalescer) -> Self {
        Self {
            root: None,
            goal: None,
            coalescer,
            starting: None,
            turns: HashMap::new(),
            order: Vec::new(),
            order_complete: false,
            items: HashMap::new(),
            requests: HashMap::new(),
            children: HashMap::new(),
            child_turns: HashMap::new(),
            terminals: BTreeMap::new(),
            rate_limits: None,
            failures: HashMap::new(),
            citations: Citations::default(),
            last_run: None,
        }
    }

    pub fn bind_root(&mut self, id: ProviderThreadId, native: &str) {
        let fresh = self.root.as_ref().is_none_or(|root| root.native != native);
        if fresh {
            self.order.clear();
            // A thread this session just started has no earlier turns; a
            // resumed one may.
            self.order_complete = false;
        }
        self.root = Some(Root {
            native: native.to_owned(),
            id,
        });
    }

    pub fn mark_history_complete(&mut self) {
        self.order_complete = true;
    }

    pub fn active_root_turn(&self) -> Option<String> {
        self.turns.keys().next().cloned()
    }

    pub fn begin_start(&mut self, req: &TurnStart) -> Result<(), NotReadyReason> {
        if self.starting.is_some() || self.turns.values().any(|turn| turn.run.is_some()) {
            return Err(NotReadyReason::PreviousTurnActive);
        }
        self.starting = Some(Starting {
            run: req.run,
            ordinal: req.provider_turn_ordinal,
            turn: None,
            cancelled: false,
        });
        Ok(())
    }

    pub fn abort_start(&mut self, run: &RunRef) {
        if self
            .starting
            .as_ref()
            .is_some_and(|starting| starting.run.attempt_id == run.attempt_id)
        {
            self.starting = None;
        }
    }

    /// Whether an interrupt arrived before the turn had a native id.
    pub fn take_cancel(&mut self, native: &str) -> bool {
        if let Some(starting) = self.starting.as_mut()
            && starting.cancelled
        {
            starting.cancelled = false;
            return true;
        }
        match self.turns.get_mut(native) {
            Some(turn) if turn.cancel => {
                turn.cancel = false;
                true
            }
            _ => false,
        }
    }

    pub fn cancel_start(&mut self, run: &RunRef) {
        if let Some(starting) = self.starting.as_mut()
            && starting.run.attempt_id == run.attempt_id
        {
            starting.cancelled = true;
        }
    }

    pub fn native_turn_of(
        &self,
        reference: Option<&ProviderTurnRef>,
        run: &RunRef,
    ) -> Option<String> {
        reference
            .map(|reference| reference.native.id.clone())
            .filter(|id| self.turns.contains_key(id))
            .or_else(|| {
                self.turns
                    .iter()
                    .find(|(_, turn)| {
                        turn.run
                            .is_some_and(|known| known.attempt_id == run.attempt_id)
                    })
                    .map(|(id, _)| id.clone())
            })
    }

    /// Native root turns after `target` (all of them for `None`), when this
    /// session knows the thread's whole tail.
    pub fn native_turns_after(&self, target: Option<&str>) -> Option<u32> {
        match target {
            Some(target) => self
                .order
                .iter()
                .position(|id| id == target)
                .map(|index| (self.order.len() - index - 1) as u32),
            None => self.order_complete.then_some(self.order.len() as u32),
        }
    }

    /// The history after a revert that dropped `before` and every later turn.
    pub fn reverted(&mut self, before: &str) -> Option<Vec<String>> {
        let index = self.order.iter().position(|id| id == before)?;
        self.order.truncate(index);
        Some(self.order.clone())
    }

    pub fn has_live_background(&self) -> bool {
        self.terminals.values().any(|terminal| terminal.live)
    }

    pub fn background_stopped(&mut self, process_id: &str) -> Vec<AdapterEvent> {
        match self.terminals.remove(process_id) {
            Some(terminal) if terminal.live => {
                vec![background_event(process_id, &terminal, false)]
            }
            _ => Vec::new(),
        }
    }

    /// The reply to a runtime request, and its resolved snapshot.
    pub fn answer(
        &mut self,
        req: &RuntimeResponse,
        now: Timestamp,
    ) -> DriverResult<(Value, Value, RuntimeRequest)> {
        let key = self
            .requests
            .contains_key(&req.request_id)
            .then_some(req.request_id)
            .or_else(|| {
                self.requests
                    .iter()
                    .find(|(_, pending)| rpc_id_string(&pending.rpc_id) == req.native.id)
                    .map(|(id, _)| *id)
            })
            .ok_or(DriverError::NotReady(NotReadyReason::NoActiveTurn))?;
        if self.requests[&key].by_message {
            // Async questions are answered with the next user message
            // (Phase 3).
            return Err(DriverError::Unsupported {
                operation: "respond",
            });
        }
        let pending = self.requests.remove(&key).expect("checked above");
        let result = match &req.reply {
            RuntimeReply::Decision { option_id } => {
                let decision = pending
                    .decisions
                    .iter()
                    .find(|decision| &decision.id == option_id)
                    .cloned()
                    .ok_or_else(|| DriverError::Protocol {
                        reason: ProtocolReason::UnexpectedFrame,
                        detail: format!("unknown Codex decision {option_id}"),
                    });
                let decision = match decision {
                    Ok(decision) => decision,
                    Err(error) => {
                        self.requests.insert(key, pending);
                        return Err(error);
                    }
                };
                protocol::approval_reply(&pending.method, &pending.params, &decision)
            }
            RuntimeReply::Answers(answers) => json!({
                "answers": answers
                    .iter()
                    .map(|answer| (answer.question_id.clone(), json!({ "answers": answer.answers })))
                    .collect::<serde_json::Map<_, _>>()
            }),
            RuntimeReply::Dismiss => {
                if pending.method == "item/tool/requestUserInput" {
                    json!({
                        "answers": pending
                            .question_ids
                            .iter()
                            .map(|id| (id.clone(), json!({ "answers": [] })))
                            .collect::<serde_json::Map<_, _>>()
                    })
                } else {
                    let decision = pending
                        .decisions
                        .iter()
                        .find(|decision| decision.id == "cancel")
                        .or_else(|| pending.decisions.iter().find(|decision| !decision.allow))
                        .cloned()
                        .unwrap_or(Decision {
                            id: "decline".into(),
                            label: String::new(),
                            allow: false,
                            value: json!("decline"),
                        });
                    protocol::approval_reply(&pending.method, &pending.params, &decision)
                }
            }
        };
        let resolved = RuntimeRequest {
            status: RequestStatus::Resolved,
            resolved_at: Some(now),
            ..pending.request
        };
        Ok((pending.rpc_id, result, resolved))
    }
}

fn rpc_id_string(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}

fn str_at<'a>(value: &'a Value, pointer: &str) -> Option<&'a str> {
    value.pointer(pointer).and_then(Value::as_str)
}

fn param<'a>(params: &'a Value, key: &str) -> &'a str {
    params.get(key).and_then(Value::as_str).unwrap_or_default()
}

fn background_event(process_id: &str, terminal: &Terminal, live: bool) -> AdapterEvent {
    AdapterEvent::BackgroundWorkUpdated {
        work: BackgroundWorkItem {
            provider_thread_id: terminal.provider_thread_id,
            task: PendingBackgroundTask {
                kind: PendingTaskKind::Command,
                task_id: process_id.to_owned(),
                description: Some(terminal.command.clone()),
                parent_task_id: Some(terminal.item.clone()),
                child_thread_id: None,
            },
            live,
        },
    }
}

/// The read loop: one frame at a time, in order.
pub(super) async fn read_loop(
    shared: Arc<Shared>,
    stdout: Box<dyn AsyncRead + Send + Unpin>,
    exit: BoxFuture<'static, std::io::Result<ProcessExit>>,
) {
    let mut lines = BufReader::new(stdout).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(frame) = serde_json::from_str::<Value>(&line) else {
            shared.conn.raw_log(FrameDirection::Inbound, &line);
            continue;
        };
        if let Some(legacy) = &shared.legacy {
            legacy.observe(&frame);
        }
        let (events, action) = handle(&shared, &frame);
        shared.emit(events).await;
        if let Some(action) = action {
            shared.act(action).await;
        }
    }
    let status = exit.await;
    shared.conn.fail_all();
    let events = {
        let mut state = shared.state.lock();
        process_exit(&mut state, &shared, &status)
    };
    shared.emit(events).await;
    if let Some(legacy) = &shared.legacy {
        legacy.exited(&status);
    }
    // Dropping the last sender ends the session's event stream.
    shared.events.lock().take();
    shared.conn.close().await;
}

/// Every open turn gets its one terminal when the process goes away.
fn process_exit(
    state: &mut State,
    shared: &Shared,
    status: &std::io::Result<ProcessExit>,
) -> Vec<AdapterEvent> {
    let mut events = state.coalescer.flush();
    let detail = match status {
        Ok(exit) if exit.success => "The Codex app-server exited.".to_owned(),
        Ok(exit) => match exit.code {
            Some(code) => format!("The Codex app-server exited with status {code}."),
            None => "The Codex app-server was stopped.".to_owned(),
        },
        Err(error) => format!("The Codex app-server failed: {error}"),
    };
    let now = shared.now();
    let mut open = state.turns.drain().collect::<Vec<_>>();
    open.sort_by_key(|(_, turn)| turn.reference.ordinal);
    for (_, turn) in open {
        events.push(AdapterEvent::ProviderTurnUpdated {
            turn: turn_snapshot(&turn, ProviderTurnStatus::Failed, now),
        });
        if let Some(run) = turn.run {
            events.push(AdapterEvent::TurnTerminal {
                terminal: TurnTerminal {
                    run,
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
    }
    // A start still in flight fails with `SessionLost` instead: only an
    // accepted turn gets a terminal.
    state.starting = None;
    state.requests.clear();
    for terminal in state.terminals.values_mut() {
        terminal.live = false;
    }
    events
}

fn turn_snapshot(turn: &OpenTurn, status: ProviderTurnStatus, now: Timestamp) -> ProviderTurn {
    let terminal = !matches!(
        status,
        ProviderTurnStatus::Running | ProviderTurnStatus::Pending
    );
    ProviderTurn {
        id: turn.reference.provider_turn_id,
        provider_thread_id: turn.provider_thread_id,
        node_id: turn
            .run
            .map(|run| run.root_node_id)
            .unwrap_or(NodeId(turn.reference.provider_turn_id.0)),
        run_attempt_id: turn.run.map(|run| run.attempt_id),
        native_turn_ref: Some(turn.reference.native.to_provider_ref(DRIVER)),
        ordinal: turn.reference.ordinal,
        status,
        started_at: Some(turn.started_at),
        completed_at: terminal.then_some(now),
        token_usage: turn.usage.clone(),
        turn_token_usage: turn.turn_usage.clone(),
    }
}

/// Applies one frame. Returns the events to send and a reply to write.
fn handle(shared: &Shared, frame: &Value) -> (Vec<AdapterEvent>, Option<ServerAction>) {
    let method = frame.get("method").and_then(Value::as_str);
    let id = frame.get("id").filter(|id| !id.is_null());
    match (method, id) {
        (None, Some(_)) => (response(shared, frame), None),
        (Some(method), Some(id)) => server_request(shared, method, id, frame),
        (Some(method), None) => {
            let params = frame.get("params").cloned().unwrap_or(Value::Null);
            let events = notification(shared, method, &params);
            match events {
                Some(events) => (events, None),
                None => {
                    shared
                        .conn
                        .raw_log(FrameDirection::Inbound, &frame.to_string());
                    (Vec::new(), None)
                }
            }
        }
        _ => {
            shared
                .conn
                .raw_log(FrameDirection::Inbound, &frame.to_string());
            (Vec::new(), None)
        }
    }
}

/// A response. `turn/start`'s opens the turn before its waiter wakes, so an
/// interrupt that follows always finds the native id.
fn response(shared: &Shared, frame: &Value) -> Vec<AdapterEvent> {
    let mut events = Vec::new();
    if shared.conn.pending_method(frame).as_deref() == Some("turn/start")
        && let Some(turn) = str_at(frame, "/result/turn/id")
    {
        let mut state = shared.state.lock();
        events = open_root_turn(&mut state, shared, turn, true);
    }
    shared.conn.complete(frame);
    events
}

fn open_root_turn(
    state: &mut State,
    shared: &Shared,
    native: &str,
    from_start: bool,
) -> Vec<AdapterEvent> {
    if state.turns.contains_key(native) {
        return Vec::new();
    }
    let Some(root) = state.root.as_ref() else {
        return Vec::new();
    };
    let provider_thread_id = root.id;
    let now = shared.now();
    let adopt = state
        .starting
        .as_ref()
        .is_some_and(|starting| starting.turn.as_deref().is_none_or(|turn| turn == native));
    let (run, ordinal, cancel) = if adopt {
        let starting = state.starting.take().expect("checked above");
        state.last_run = Some(starting.run);
        (Some(starting.run), starting.ordinal, starting.cancelled)
    } else if from_start {
        return Vec::new();
    } else {
        // Codex started this turn itself (a `/goal` continuation). Behind
        // the bridge it gets a run of its own so the desktop sees it start
        // and finish, as before; the orchestrator decides its run (#59).
        let run = shared.legacy.as_ref().map(|_| RunRef {
            thread_id: shared.thread_id,
            run_id: RunId(shared.ids.fresh(EntityKind::Node)),
            attempt_id: RunAttemptId(shared.ids.fresh(EntityKind::Node)),
            root_node_id: NodeId(shared.ids.fresh(EntityKind::Node)),
        });
        let ordinal = state.order.len() as u32 + 1;
        (run, ordinal, false)
    };
    let turn = OpenTurn {
        run,
        reference: ProviderTurnRef {
            provider_turn_id: shared.bind(provider_thread_id.0, EntityKind::ProviderTurn, native),
            native: NativeRef::strong(native),
            ordinal,
        },
        provider_thread_id,
        started_at: now,
        cancel,
        usage: None,
        turn_usage: None,
    };
    state.citations.reset();
    state.order.push(native.to_owned());
    let snapshot = turn_snapshot(&turn, ProviderTurnStatus::Running, now);
    state.turns.insert(native.to_owned(), turn);
    state
        .coalescer
        .offer(AdapterEvent::ProviderTurnUpdated { turn: snapshot })
}

fn is_root(state: &State, thread: &str) -> bool {
    state
        .root
        .as_ref()
        .is_some_and(|root| root.native == thread)
}

/// Normalizes a notification. `None` for an unknown method.
fn notification(shared: &Shared, method: &str, params: &Value) -> Option<Vec<AdapterEvent>> {
    let mut guard = shared.state.lock();
    let state = &mut *guard;
    let thread = param(params, "threadId");
    let events = match method {
        "turn/started" => {
            let turn = str_at(params, "/turn/id")?;
            if is_root(state, thread) {
                open_root_turn(state, shared, turn, false)
            } else if state.children.contains_key(thread) {
                child_turn_started(state, shared, thread, turn)
            } else {
                Vec::new()
            }
        }
        "turn/completed" => {
            let turn = params.get("turn")?;
            let native = turn.get("id").and_then(Value::as_str)?;
            if state.turns.contains_key(native) {
                root_turn_completed(state, shared, native, turn)
            } else if state.child_turns.contains_key(native) {
                child_turn_completed(state, shared, native, turn)
            } else {
                Vec::new()
            }
        }
        "item/started" | "item/completed" => {
            item(state, shared, params, method == "item/completed")
        }
        "item/agentMessage/delta" => text_delta(state, shared, params),
        "item/reasoning/summaryTextDelta" => reasoning_delta(state, shared, params, true),
        "item/reasoning/textDelta" => reasoning_delta(state, shared, params, false),
        "item/commandExecution/outputDelta" => output_delta(state, shared, params),
        "turn/plan/updated" => plan_updated(state, shared, params),
        "thread/tokenUsage/updated" => token_usage(state, shared, params),
        "account/rateLimits/updated" => {
            let update = params.get("rateLimits")?;
            state.rate_limits = protocol::merge_rate_limits(state.rate_limits.as_ref(), update);
            match state.rate_limits.as_ref().and_then(protocol::plan_usage) {
                Some(usage) => state
                    .coalescer
                    .offer(AdapterEvent::AccountUsageUpdated { usage }),
                None => Vec::new(),
            }
        }
        "thread/goal/updated" if is_root(state, thread) => {
            let goal = params.get("goal").and_then(protocol::parse_goal);
            state.goal = goal.clone();
            state.coalescer.offer(AdapterEvent::GoalUpdated { goal })
        }
        "thread/goal/cleared" if is_root(state, thread) => {
            state.goal = None;
            state
                .coalescer
                .offer(AdapterEvent::GoalUpdated { goal: None })
        }
        "thread/name/updated" if is_root(state, thread) && shared.legacy.is_some() => {
            let root = state.root.as_ref()?;
            let title = params
                .get("threadName")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let row = shared.thread_row(root.id, &root.native, None, title);
            state.coalescer.offer(AdapterEvent::ProviderThreadUpdated {
                thread: row,
                retained_native_turn_ids: None,
            })
        }
        "error" => {
            // Kept for the terminal: `turn/completed` may carry only the
            // message.
            if params.get("willRetry").and_then(Value::as_bool) != Some(true)
                && let (Some(turn), Some(error)) = (
                    params.get("turnId").and_then(Value::as_str),
                    params.get("error"),
                )
            {
                state.failures.insert(turn.to_owned(), error.clone());
            }
            Vec::new()
        }
        "serverRequest/resolved" => {
            let request = params.get("requestId")?;
            let resolved = state
                .requests
                .iter()
                .find(|(_, pending)| &pending.rpc_id == request)
                .map(|(id, _)| *id);
            match resolved.and_then(|id| state.requests.remove(&id)) {
                Some(pending) => {
                    let now = shared.now();
                    state.coalescer.offer(AdapterEvent::RuntimeRequestUpdated {
                        request: RuntimeRequest {
                            status: RequestStatus::Resolved,
                            resolved_at: Some(now),
                            ..pending.request
                        },
                    })
                }
                None => Vec::new(),
            }
        }
        "mcpServer/startupStatus/updated" => {
            if param(params, "status") != "failed" {
                return Some(Vec::new());
            }
            let name = params.get("name").and_then(Value::as_str).unwrap_or("MCP");
            let error = params
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("failed to start");
            let now = shared.now();
            let session = ProviderSession {
                id: shared.session_id,
                driver: DRIVER,
                provider_instance_id: shared.instance.instance_id.clone(),
                status: SessionStatus::Ready,
                cwd: shared.cwd.clone().into(),
                model: shared.selection.lock().model.clone(),
                capabilities: shared.capabilities.clone(),
                created_at: shared.created_at,
                updated_at: now,
                last_error: Some(format!("{name}: {error}")),
                detached_at: None,
                detach_reason: None,
            };
            state
                .coalescer
                .offer(AdapterEvent::SessionUpdated { session })
        }
        // Known frames with nothing to map.
        "thread/started"
        | "thread/status/changed"
        | "thread/settings/updated"
        | "thread/reverted"
        | "thread/goal/updated"
        | "thread/goal/cleared"
        | "thread/name/updated"
        | "thread/archived"
        | "thread/unarchived"
        | "thread/closed"
        | "configWarning"
        | "deprecationNotice"
        | "remoteControl/status/changed"
        | "account/updated"
        | "skills/changed"
        | "turn/diff/updated"
        | "item/reasoning/summaryPartAdded"
        | "item/fileChange/outputDelta"
        | "item/fileChange/patchUpdated"
        | "item/plan/delta"
        | "item/mcpToolCall/progress" => Vec::new(),
        _ => return None,
    };
    Some(events)
}

/// The context a row of `turn` is written in: a root turn's run, or the run
/// that spawned a subagent (its node and provider thread).
struct RowContext {
    run: RunRef,
    node: NodeId,
    provider_turn_id: ProviderTurnId,
    provider_thread_id: ProviderThreadId,
}

impl RowContext {
    fn of(state: &State, turn: &str) -> Option<Self> {
        if let Some(open) = state.turns.get(turn) {
            let run = open.run?;
            return Some(Self {
                run,
                node: run.root_node_id,
                provider_turn_id: open.reference.provider_turn_id,
                provider_thread_id: open.provider_thread_id,
            });
        }
        let child_turn = state.child_turns.get(turn)?;
        let child = state.children.get(&child_turn.child)?;
        Some(Self {
            run: child.parent,
            node: child.node_id,
            provider_turn_id: child_turn.reference.provider_turn_id,
            provider_thread_id: child.provider_thread_id,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn row(
        &self,
        id: TurnItemId,
        native: &str,
        node: Option<NodeId>,
        started_at: Timestamp,
        now: Timestamp,
        status: ItemStatus,
        title: Option<String>,
        kind: TurnItemKind,
    ) -> TurnItem {
        TurnItem {
            id,
            thread_id: self.run.thread_id,
            run_id: Some(self.run.run_id),
            node_id: Some(node.unwrap_or(self.node)),
            provider_thread_id: Some(self.provider_thread_id),
            provider_turn_id: Some(self.provider_turn_id),
            native_item_ref: Some(ProviderRef::strong(DRIVER, native)),
            parent_item_id: None,
            position: 0,
            status,
            title,
            tool: None,
            stream_rev: 0,
            started_at: Some(started_at),
            completed_at: status.is_terminal().then_some(now),
            updated_at: now,
            kind,
        }
    }

    fn tool_node(
        &self,
        id: NodeId,
        native: &str,
        status: ItemStatus,
        started_at: Timestamp,
        now: Timestamp,
    ) -> ExecutionNode {
        ExecutionNode {
            id,
            thread_id: self.run.thread_id,
            run_id: Some(self.run.run_id),
            parent_node_id: Some(self.node),
            root_node_id: self.run.root_node_id,
            kind: NodeKind::ToolCall,
            status: node_status(status),
            counts_for_run: false,
            provider_thread_id: Some(self.provider_thread_id),
            provider_turn_id: Some(self.provider_turn_id),
            native_item_ref: Some(ProviderRef::strong(DRIVER, native)),
            runtime_request_id: None,
            checkpoint_scope_id: None,
            started_at: Some(started_at),
            completed_at: status.is_terminal().then_some(now),
        }
    }

    fn message(
        &self,
        id: MessageId,
        text: String,
        streaming: bool,
        started_at: Timestamp,
        now: Timestamp,
    ) -> Message {
        Message {
            id,
            thread_id: self.run.thread_id,
            run_id: Some(self.run.run_id),
            node_id: Some(self.run.root_node_id),
            role: MessageRole::Assistant,
            created_by: Actor::Agent,
            creation_source: CreationSource::Provider,
            text,
            display_text: None,
            attachments: Vec::new(),
            context: Vec::new(),
            notification: None,
            sender_thread_id: None,
            streaming,
            created_at: started_at,
            updated_at: now,
        }
    }
}

fn node_status(status: ItemStatus) -> NodeStatus {
    match status {
        ItemStatus::Completed => NodeStatus::Completed,
        ItemStatus::Failed => NodeStatus::Failed,
        ItemStatus::Cancelled => NodeStatus::Cancelled,
        ItemStatus::Interrupted => NodeStatus::Interrupted,
        ItemStatus::Waiting => NodeStatus::Waiting,
        _ => NodeStatus::Running,
    }
}

fn item_status(item: &Value, completed: bool) -> ItemStatus {
    if !completed {
        return ItemStatus::Running;
    }
    match item.get("status").and_then(Value::as_str) {
        Some("failed") => ItemStatus::Failed,
        Some("declined") => ItemStatus::Cancelled,
        Some("interrupted") => ItemStatus::Interrupted,
        _ if item.get("success").and_then(Value::as_bool) == Some(false)
            || item.get("error").is_some_and(|error| !error.is_null()) =>
        {
            ItemStatus::Failed
        }
        _ => ItemStatus::Completed,
    }
}

fn opened<'a>(
    state: &'a mut State,
    native: &str,
    turn: &str,
    kind: ItemKind,
    now: Timestamp,
) -> &'a mut OpenItem {
    state
        .items
        .entry(native.to_owned())
        .or_insert_with(|| OpenItem {
            turn: turn.to_owned(),
            kind,
            text: String::new(),
            part: None,
            started_at: now,
            completed: false,
            process_id: None,
            command: String::new(),
            streamed: false,
        })
}

fn json_text(value: Option<&Value>) -> JsonText {
    JsonText(
        value
            .map(Value::to_string)
            .unwrap_or_else(|| "{}".to_owned()),
    )
}

fn offer_all(state: &mut State, events: Vec<AdapterEvent>) -> Vec<AdapterEvent> {
    events
        .into_iter()
        .flat_map(|event| state.coalescer.offer(event))
        .collect()
}

fn item(state: &mut State, shared: &Shared, params: &Value, completed: bool) -> Vec<AdapterEvent> {
    let Some(item) = params.get("item") else {
        return Vec::new();
    };
    let native = item.get("id").and_then(Value::as_str).unwrap_or_default();
    let kind = item.get("type").and_then(Value::as_str).unwrap_or_default();
    let thread = param(params, "threadId");
    let turn = param(params, "turnId");
    let now = shared.now();
    if !is_root(state, thread) {
        // A subagent's final answer is its result.
        if let Some(child) = state.children.get_mut(thread)
            && completed
            && kind == "agentMessage"
            && let Some(text) = item.get("text").and_then(Value::as_str)
        {
            child.result = Some(text.to_owned());
        }
        return Vec::new();
    }
    if kind == "webSearch" {
        state.citations.capture(item);
    }
    match kind {
        "agentMessage" => assistant_item(state, shared, item, native, turn, completed, now),
        "reasoning" => reasoning_item(state, shared, item, native, turn, completed, now),
        "commandExecution" => command_item(state, shared, item, native, turn, completed, now),
        "fileChange" | "mcpToolCall" | "dynamicToolCall" | "imageView" | "webSearch" => {
            tool_item(state, shared, item, native, turn, completed, now)
        }
        "collabAgentToolCall" => collab_item(state, shared, item, turn, now),
        "subAgentActivity" => subagent_activity(state, shared, item, turn, now),
        _ => Vec::new(),
    }
}

fn assistant_item(
    state: &mut State,
    shared: &Shared,
    item: &Value,
    native: &str,
    turn: &str,
    completed: bool,
    now: Timestamp,
) -> Vec<AdapterEvent> {
    opened(state, native, turn, ItemKind::Assistant, now);
    if !completed {
        return Vec::new();
    }
    let open = state.items.remove(native).expect("opened above");
    let Some(cx) = RowContext::of(state, turn) else {
        return Vec::new();
    };
    let provided = item.get("text").and_then(Value::as_str).unwrap_or_default();
    let rest = state.citations.buffers.remove(native).unwrap_or_default();
    let text = if provided.is_empty() {
        format!("{}{}", open.text, state.citations.full(&rest))
    } else {
        state.citations.full(provided)
    };
    let pt = cx.provider_thread_id.0;
    let item_id: TurnItemId = shared.bind(pt, EntityKind::Item, native);
    let message_id: MessageId = shared.bind(pt, EntityKind::Message, native);
    let mut events = Vec::new();
    if !text.is_empty() || open.streamed {
        events.push(AdapterEvent::MessageUpdated {
            message: cx.message(message_id, text.clone(), false, open.started_at, now),
        });
        events.push(AdapterEvent::TurnItemUpdated {
            item: cx.row(
                item_id,
                native,
                None,
                open.started_at,
                now,
                ItemStatus::Completed,
                None,
                TurnItemKind::AssistantMessage {
                    message_id,
                    text,
                    streaming: false,
                },
            ),
        });
    }
    // An async question is answered with the next user message (Phase 3).
    if item.get("delivery").and_then(Value::as_str) == Some("async")
        && let Some(questions) = item
            .get("questions")
            .and_then(Value::as_array)
            .filter(|questions| !questions.is_empty())
    {
        events.extend(async_question(state, shared, &cx, native, questions, now));
    }
    offer_all(state, events)
}

fn async_question(
    state: &mut State,
    shared: &Shared,
    cx: &RowContext,
    native: &str,
    questions: &[Value],
    now: Timestamp,
) -> Vec<AdapterEvent> {
    let key = format!("async:{native}");
    let request_id: RuntimeRequestId =
        shared.bind(cx.provider_thread_id.0, EntityKind::Request, &key);
    let questions = questions
        .iter()
        .enumerate()
        .map(|(index, question)| UserInputQuestion {
            id: index.to_string(),
            header: "Question".to_owned(),
            question: question
                .get("title")
                .or_else(|| question.get("question"))
                .and_then(Value::as_str)
                .unwrap_or("Choose an answer.")
                .to_owned(),
            options: question
                .get("options")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|option| {
                    let label = option
                        .as_str()
                        .or_else(|| option.get("label").and_then(Value::as_str))?;
                    Some(UserInputOption {
                        label: label.to_owned(),
                        description: None,
                    })
                })
                .collect(),
            multi_select: false,
        })
        .collect::<Vec<_>>();
    let request = RuntimeRequest {
        id: request_id,
        thread_id: cx.run.thread_id,
        node_id: cx.node,
        provider_turn_id: Some(cx.provider_turn_id),
        native_request_ref: Some(ProviderRef::strong(DRIVER, &key)),
        kind: RuntimeRequestKind::UserInput,
        status: RequestStatus::Pending,
        response_capability: ResponseCapability::Message,
        options: Vec::new(),
        questions: questions.clone(),
        response: None,
        created_at: now,
        resolved_at: None,
    };
    state.requests.insert(
        request_id,
        PendingRequest {
            rpc_id: Value::String(key.clone()),
            method: "async".to_owned(),
            params: Value::Null,
            decisions: Vec::new(),
            request: request.clone(),
            question_ids: Vec::new(),
            by_message: true,
        },
    );
    vec![
        AdapterEvent::TurnItemUpdated {
            item: cx.row(
                derived::request_item(request_id),
                &key,
                None,
                now,
                now,
                ItemStatus::Waiting,
                None,
                TurnItemKind::UserInputRequest {
                    request_id,
                    questions,
                    answers: None,
                    response_mode: Some(ResponseMode::Message),
                },
            ),
        },
        AdapterEvent::RuntimeRequestUpdated { request },
    ]
}

fn text_delta(state: &mut State, shared: &Shared, params: &Value) -> Vec<AdapterEvent> {
    let thread = param(params, "threadId");
    if !is_root(state, thread) {
        return Vec::new();
    }
    let native = param(params, "itemId").to_owned();
    let turn = param(params, "turnId").to_owned();
    let now = shared.now();
    let delta = state.citations.delta(&native, param(params, "delta"));
    let open = opened(state, &native, &turn, ItemKind::Assistant, now);
    if delta.is_empty() {
        return Vec::new();
    }
    open.text.push_str(&delta);
    open.streamed = true;
    let (text, started_at) = (open.text.clone(), open.started_at);
    let Some(cx) = RowContext::of(state, &turn) else {
        return Vec::new();
    };
    let pt = cx.provider_thread_id.0;
    let item_id: TurnItemId = shared.bind(pt, EntityKind::Item, &native);
    let message_id: MessageId = shared.bind(pt, EntityKind::Message, &native);
    let events = vec![
        AdapterEvent::MessageUpdated {
            message: cx.message(message_id, text.clone(), true, started_at, now),
        },
        AdapterEvent::TurnItemUpdated {
            item: cx.row(
                item_id,
                &native,
                None,
                started_at,
                now,
                ItemStatus::Running,
                None,
                TurnItemKind::AssistantMessage {
                    message_id,
                    text,
                    streaming: true,
                },
            ),
        },
    ];
    offer_all(state, events)
}

fn reasoning_delta(
    state: &mut State,
    shared: &Shared,
    params: &Value,
    summary: bool,
) -> Vec<AdapterEvent> {
    let thread = param(params, "threadId");
    let delta = param(params, "delta");
    if !is_root(state, thread) || delta.is_empty() {
        return Vec::new();
    }
    let native = param(params, "itemId").to_owned();
    let turn = param(params, "turnId").to_owned();
    let now = shared.now();
    let index = params
        .get(if summary {
            "summaryIndex"
        } else {
            "contentIndex"
        })
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let open = opened(state, &native, &turn, ItemKind::Reasoning, now);
    // Codex numbers reasoning parts but their deltas carry no separator; run
    // together, the bold part headers would fuse.
    let part = (summary, index);
    if open.part.is_some_and(|previous| previous != part) && !open.text.is_empty() {
        open.text.push_str("\n\n");
    }
    open.part = Some(part);
    open.text.push_str(delta);
    open.streamed = true;
    let (text, started_at) = (open.text.clone(), open.started_at);
    let Some(cx) = RowContext::of(state, &turn) else {
        return Vec::new();
    };
    let item_id: TurnItemId = shared.bind(cx.provider_thread_id.0, EntityKind::Item, &native);
    state.coalescer.offer(AdapterEvent::TurnItemUpdated {
        item: cx.row(
            item_id,
            &native,
            None,
            started_at,
            now,
            ItemStatus::Running,
            None,
            TurnItemKind::Reasoning {
                text,
                streaming: true,
            },
        ),
    })
}

fn reasoning_item(
    state: &mut State,
    shared: &Shared,
    item: &Value,
    native: &str,
    turn: &str,
    completed: bool,
    now: Timestamp,
) -> Vec<AdapterEvent> {
    opened(state, native, turn, ItemKind::Reasoning, now);
    if !completed {
        return Vec::new();
    }
    let open = state.items.remove(native).expect("opened above");
    let Some(cx) = RowContext::of(state, turn) else {
        return Vec::new();
    };
    let joined = |key: &str| {
        item.get(key)
            .and_then(Value::as_array)
            .map(|parts| {
                parts
                    .iter()
                    .filter_map(|part| {
                        part.as_str()
                            .or_else(|| part.get("text").and_then(Value::as_str))
                    })
                    .filter(|part| !part.is_empty())
                    .collect::<Vec<_>>()
                    .join("\n\n")
            })
            .unwrap_or_default()
    };
    let mut text = joined("summary");
    if text.is_empty() {
        text = joined("content");
    }
    if text.is_empty() {
        text = open.text;
    }
    if text.is_empty() && !open.streamed {
        return Vec::new();
    }
    let item_id: TurnItemId = shared.bind(cx.provider_thread_id.0, EntityKind::Item, native);
    state.coalescer.offer(AdapterEvent::TurnItemUpdated {
        item: cx.row(
            item_id,
            native,
            None,
            open.started_at,
            now,
            ItemStatus::Completed,
            None,
            TurnItemKind::Reasoning {
                text,
                streaming: false,
            },
        ),
    })
}

fn command_kind(item: &Value, completed: bool) -> TurnItemKind {
    let exit_code = item
        .get("exitCode")
        .and_then(Value::as_i64)
        .and_then(|code| i32::try_from(code).ok());
    TurnItemKind::CommandExecution {
        input: item
            .get("command")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        output: completed
            .then(|| item.get("aggregatedOutput").and_then(Value::as_str))
            .flatten()
            .map(str::to_owned),
        output_truncated: false,
        output_omitted: false,
        output_indicates_failure: exit_code.is_some_and(|code| code != 0)
            || item.get("status").and_then(Value::as_str) == Some("failed"),
        exit_code: if completed { exit_code } else { None },
    }
}

fn process_id_of(item: &Value) -> Option<String> {
    item.get("processId").and_then(|id| {
        id.as_str()
            .map(str::to_owned)
            .or_else(|| id.as_u64().map(|id| id.to_string()))
    })
}

fn command_item(
    state: &mut State,
    shared: &Shared,
    item: &Value,
    native: &str,
    turn: &str,
    completed: bool,
    now: Timestamp,
) -> Vec<AdapterEvent> {
    let process_id = process_id_of(item);
    let command = item
        .get("command")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    if !state.turns.contains_key(turn) && !state.child_turns.contains_key(turn) {
        // The turn ended while the command kept running: a background
        // terminal that has now finished.
        if completed {
            state.items.remove(native);
            if let Some(process) = process_id.as_deref()
                && let Some(terminal) = state.terminals.remove(process)
                && terminal.live
            {
                let event = background_event(process, &terminal, false);
                return state.coalescer.offer(event);
            }
        }
        return Vec::new();
    }
    let started_at = {
        let open = opened(state, native, turn, ItemKind::Command, now);
        open.process_id = process_id.clone();
        open.command = command.clone();
        open.completed = completed;
        open.started_at
    };
    if completed {
        state.items.remove(native);
    }
    let Some(cx) = RowContext::of(state, turn) else {
        return Vec::new();
    };
    if let Some(process) = &process_id {
        if completed {
            state.terminals.remove(process);
        } else {
            state.terminals.entry(process.clone()).or_insert(Terminal {
                item: native.to_owned(),
                command: command.clone(),
                provider_thread_id: cx.provider_thread_id,
                live: false,
            });
        }
    }
    let status = item_status(item, completed);
    let pt = cx.provider_thread_id.0;
    let item_id: TurnItemId = shared.bind(pt, EntityKind::Item, native);
    let node_id: NodeId = shared.bind(pt, EntityKind::Node, native);
    let events = vec![
        AdapterEvent::NodeUpdated {
            node: cx.tool_node(node_id, native, status, started_at, now),
        },
        AdapterEvent::TurnItemUpdated {
            item: cx.row(
                item_id,
                native,
                Some(node_id),
                started_at,
                now,
                status,
                None,
                command_kind(item, completed),
            ),
        },
    ];
    offer_all(state, events)
}

fn diff_counts(diff: &str) -> (u32, u32) {
    let mut additions = 0;
    let mut deletions = 0;
    for line in diff.lines() {
        if line.starts_with('+') && !line.starts_with("+++") {
            additions += 1;
        } else if line.starts_with('-') && !line.starts_with("---") {
            deletions += 1;
        }
    }
    (additions, deletions)
}

fn file_change_kind(item: &Value) -> TurnItemKind {
    let changes = item
        .get("changes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let diff = changes
        .iter()
        .filter_map(|change| change.get("diff").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    let (additions, deletions) = diff_counts(&diff);
    let details = changes
        .iter()
        .filter_map(|change| {
            let path = change.get("path").and_then(Value::as_str)?;
            Some(FileChangeDetail {
                operation: change
                    .pointer("/kind/type")
                    .and_then(Value::as_str)
                    .unwrap_or("update")
                    .to_owned(),
                path: path.to_owned(),
                old_path: None,
                file_type: None,
                mime_type: None,
            })
        })
        .collect::<Vec<_>>();
    let has_diff = !diff.is_empty();
    TurnItemKind::FileChange {
        file_name: details
            .first()
            .map(|detail| detail.path.clone())
            .unwrap_or_default(),
        additions: has_diff.then_some(additions),
        deletions: has_diff.then_some(deletions),
        diff: has_diff.then_some(diff),
        old_text: None,
        new_text: None,
        changes: details,
        body_omitted: false,
    }
}

fn web_search_kind(item: &Value) -> TurnItemKind {
    let mut patterns = Vec::new();
    if let Some(query) = item
        .get("query")
        .and_then(Value::as_str)
        .filter(|query| !query.trim().is_empty())
    {
        patterns.push(query.to_owned());
    }
    if patterns.is_empty() {
        patterns.extend(
            item.pointer("/action/queries")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned),
        );
    }
    let results = item
        .get("results")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|result| WebSearchResult {
            title: result
                .get("title")
                .and_then(Value::as_str)
                .map(str::to_owned),
            url: result.get("url").and_then(Value::as_str).map(str::to_owned),
            snippet: None,
        })
        .collect();
    TurnItemKind::WebSearch { patterns, results }
}

/// MCP, dynamic and image-view tools. The title is the provider's own
/// `arguments.title` when it gives one.
fn dynamic_tool_kind(item: &Value, completed: bool) -> (TurnItemKind, Option<String>) {
    let kind = item.get("type").and_then(Value::as_str).unwrap_or_default();
    let (tool, input, output, image) = match kind {
        "imageView" => (
            Some("view_image".to_owned()),
            Some(json!({ "path": item.get("path") })),
            None,
            item.get("path")
                .and_then(Value::as_str)
                .map(std::path::PathBuf::from),
        ),
        "dynamicToolCall" => (
            item.get("tool").and_then(Value::as_str).map(str::to_owned),
            item.get("arguments").cloned(),
            item.get("contentItems")
                .filter(|value| !value.is_null())
                .cloned(),
            None,
        ),
        _ => {
            let server = item.get("server").and_then(Value::as_str);
            let tool = item.get("tool").and_then(Value::as_str);
            let output = item
                .get("error")
                .filter(|error| !error.is_null())
                .or_else(|| item.get("result").filter(|result| !result.is_null()))
                .cloned();
            (
                tool.map(|tool| match server {
                    Some(server) => format!("{server}.{tool}"),
                    None => tool.to_owned(),
                }),
                item.get("arguments").cloned(),
                output,
                None,
            )
        }
    };
    let title = item
        .pointer("/arguments/title")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|title| !title.is_empty())
        .map(str::to_owned);
    let output = if completed {
        output.as_ref().map(|output| json_text(Some(output)))
    } else {
        None
    };
    (
        TurnItemKind::DynamicTool {
            tool_name: tool,
            input: json_text(input.as_ref()),
            output,
            output_omitted: false,
            viewed_image_path: image,
        },
        title,
    )
}

fn tool_item(
    state: &mut State,
    shared: &Shared,
    item: &Value,
    native: &str,
    turn: &str,
    completed: bool,
    now: Timestamp,
) -> Vec<AdapterEvent> {
    let item_kind = match item.get("type").and_then(Value::as_str) {
        Some("fileChange") => ItemKind::FileChange,
        Some("webSearch") => ItemKind::WebSearch,
        _ => ItemKind::Tool,
    };
    let started_at = {
        let open = opened(state, native, turn, item_kind, now);
        open.completed = completed;
        open.started_at
    };
    if completed {
        state.items.remove(native);
    }
    let Some(cx) = RowContext::of(state, turn) else {
        return Vec::new();
    };
    let status = item_status(item, completed);
    let pt = cx.provider_thread_id.0;
    let item_id: TurnItemId = shared.bind(pt, EntityKind::Item, native);
    let node_id: NodeId = shared.bind(pt, EntityKind::Node, native);
    let (kind, title) = match item_kind {
        ItemKind::FileChange => (file_change_kind(item), None),
        ItemKind::WebSearch => (web_search_kind(item), None),
        _ => dynamic_tool_kind(item, completed),
    };
    let events = vec![
        AdapterEvent::NodeUpdated {
            node: cx.tool_node(node_id, native, status, started_at, now),
        },
        AdapterEvent::TurnItemUpdated {
            item: cx.row(
                item_id,
                native,
                Some(node_id),
                started_at,
                now,
                status,
                title,
                kind,
            ),
        },
    ];
    offer_all(state, events)
}

fn output_delta(state: &mut State, shared: &Shared, params: &Value) -> Vec<AdapterEvent> {
    let native = param(params, "itemId");
    let delta = param(params, "delta");
    if native.is_empty() || delta.is_empty() {
        return Vec::new();
    }
    let provider_thread = state
        .items
        .get(native)
        .and_then(|open| RowContext::of(state, &open.turn))
        .map(|cx| cx.provider_thread_id)
        .or_else(|| {
            state
                .terminals
                .values()
                .find(|terminal| terminal.item == native)
                .map(|terminal| terminal.provider_thread_id)
        })
        .or_else(|| state.root.as_ref().map(|root| root.id));
    let Some(provider_thread) = provider_thread else {
        return Vec::new();
    };
    let item: TurnItemId = shared.bind(provider_thread.0, EntityKind::Item, native);
    state.coalescer.offer(AdapterEvent::CommandOutputDelta {
        item,
        chunk: delta.as_bytes().to_vec(),
        replace: false,
    })
}

fn plan_updated(state: &mut State, shared: &Shared, params: &Value) -> Vec<AdapterEvent> {
    let turn = param(params, "turnId");
    let Some(cx) = RowContext::of(state, turn) else {
        return Vec::new();
    };
    let steps = params
        .get("plan")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
        .map(|(index, step)| PlanStep {
            id: index.to_string(),
            text: param(step, "step").to_owned(),
            status: match param(step, "status") {
                "completed" => PlanStepStatus::Completed,
                "inProgress" | "in_progress" => PlanStepStatus::Running,
                _ => PlanStepStatus::Pending,
            },
        })
        .collect::<Vec<_>>();
    let done = !steps.is_empty()
        && steps
            .iter()
            .all(|step| step.status == PlanStepStatus::Completed);
    let plan = Plan {
        id: shared.bind(cx.provider_thread_id.0, EntityKind::Plan, turn),
        thread_id: cx.run.thread_id,
        run_id: Some(cx.run.run_id),
        node_id: cx.node,
        status: if done {
            PlanStatus::Completed
        } else {
            PlanStatus::Active
        },
        kind: PlanKind::TodoList {
            steps,
            explanation: params
                .get("explanation")
                .and_then(Value::as_str)
                .filter(|text| !text.trim().is_empty())
                .map(str::to_owned),
        },
    };
    state.coalescer.offer(AdapterEvent::PlanUpdated { plan })
}

fn usage_of(value: Option<&Value>, window: Option<u64>, now: Timestamp) -> Option<TokenUsage> {
    let value = value?;
    let field = |key: &str| value.get(key).and_then(Value::as_u64);
    Some(TokenUsage {
        used: field("totalTokens")?,
        max: window,
        input: field("inputTokens"),
        cached_input: field("cachedInputTokens"),
        output: field("outputTokens"),
        reasoning_output: field("reasoningOutputTokens"),
        prompt_cache_ttl_ms: None,
        updated_at: now,
    })
}

fn token_usage(state: &mut State, shared: &Shared, params: &Value) -> Vec<AdapterEvent> {
    let turn = param(params, "turnId");
    let usage = params.get("tokenUsage");
    let window = usage
        .and_then(|usage| usage.get("modelContextWindow"))
        .and_then(Value::as_u64)
        .filter(|window| *window > 0);
    let now = shared.now();
    let Some(open) = state.turns.get_mut(turn) else {
        return Vec::new();
    };
    // `last` is the newest model call, so its total is what occupies the
    // context window now; `total` sums the whole thread.
    let last = usage_of(usage.and_then(|usage| usage.get("last")), window, now);
    let Some(last) = last.filter(|last| last.used > 0 || window.is_some()) else {
        return Vec::new();
    };
    let total = usage_of(usage.and_then(|usage| usage.get("total")), window, now);
    open.usage = Some(TokenUsage {
        input: total.as_ref().and_then(|total| total.input),
        cached_input: total.as_ref().and_then(|total| total.cached_input),
        output: total.as_ref().and_then(|total| total.output),
        reasoning_output: total.as_ref().and_then(|total| total.reasoning_output),
        ..last.clone()
    });
    open.turn_usage = Some(last);
    let snapshot = turn_snapshot(open, ProviderTurnStatus::Running, now);
    state
        .coalescer
        .offer(AdapterEvent::ProviderTurnUpdated { turn: snapshot })
}

fn root_turn_completed(
    state: &mut State,
    shared: &Shared,
    native: &str,
    turn: &Value,
) -> Vec<AdapterEvent> {
    let now = shared.now();
    // Flush before the terminal (RFC 0002 rule 7).
    let mut events = state.coalescer.flush();
    let Some(open) = state.turns.remove(native) else {
        return events;
    };
    let status = turn
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("completed");
    let mut error = turn.get("error").filter(|error| !error.is_null()).cloned();
    if let Some(reported) = state.failures.remove(native) {
        match error.as_mut() {
            Some(Value::Object(map)) if map.get("codexErrorInfo").is_none_or(Value::is_null) => {
                if let Some(info) = reported.get("codexErrorInfo") {
                    map.insert("codexErrorInfo".to_owned(), info.clone());
                }
            }
            None => error = Some(reported),
            _ => {}
        }
    }
    let outcome = match status {
        "completed" => TurnOutcome::Completed,
        "interrupted" => TurnOutcome::Interrupted,
        "cancelled" => TurnOutcome::Cancelled,
        _ => TurnOutcome::Failed {
            failure: protocol::classify(
                error.as_ref().unwrap_or(&Value::Null),
                protocol::usage_limit_reset_at(state.rate_limits.as_ref()),
            ),
        },
    };
    let provider_status = match &outcome {
        TurnOutcome::Completed => ProviderTurnStatus::Completed,
        TurnOutcome::Interrupted => ProviderTurnStatus::Interrupted,
        TurnOutcome::Cancelled => ProviderTurnStatus::Cancelled,
        TurnOutcome::Failed { .. } => ProviderTurnStatus::Failed,
    };
    // Tools still open: a command Codex keeps running past a completed turn
    // is background work; any other open tool ended with the turn.
    let mut still_open = state
        .items
        .iter()
        .filter(|(_, item)| item.turn == native && !item.completed)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    still_open.sort();
    for id in still_open {
        let item = state.items.remove(&id).expect("listed above");
        if let Some(process) = &item.process_id {
            if matches!(outcome, TurnOutcome::Completed)
                && let Some(terminal) = state.terminals.get_mut(process)
            {
                terminal.live = true;
                events.push(background_event(process, terminal, true));
                continue;
            }
            state.terminals.remove(process);
        }
        let Some(run) = open.run else { continue };
        if !matches!(
            item.kind,
            ItemKind::Command | ItemKind::FileChange | ItemKind::Tool | ItemKind::WebSearch
        ) {
            continue;
        }
        let status = match &outcome {
            TurnOutcome::Failed { .. } => ItemStatus::Failed,
            _ => ItemStatus::Interrupted,
        };
        let cx = RowContext {
            run,
            node: run.root_node_id,
            provider_turn_id: open.reference.provider_turn_id,
            provider_thread_id: open.provider_thread_id,
        };
        let pt = open.provider_thread_id.0;
        let item_id: TurnItemId = shared.bind(pt, EntityKind::Item, &id);
        let node_id: NodeId = shared.bind(pt, EntityKind::Node, &id);
        events.push(AdapterEvent::NodeUpdated {
            node: cx.tool_node(node_id, &id, status, item.started_at, now),
        });
        if item.kind == ItemKind::Command {
            events.push(AdapterEvent::TurnItemUpdated {
                item: cx.row(
                    item_id,
                    &id,
                    Some(node_id),
                    item.started_at,
                    now,
                    status,
                    None,
                    TurnItemKind::CommandExecution {
                        input: item.command.clone(),
                        output: None,
                        output_truncated: false,
                        output_omitted: false,
                        output_indicates_failure: false,
                        exit_code: None,
                    },
                ),
            });
        }
    }
    events.push(AdapterEvent::ProviderTurnUpdated {
        turn: turn_snapshot(&open, provider_status, now),
    });
    if let Some(run) = open.run {
        events.push(AdapterEvent::TurnTerminal {
            terminal: TurnTerminal {
                run,
                provider_turn: Some(open.reference.clone()),
                outcome,
                thread_disposition: ThreadDisposition::Reusable,
                child_turns_continue: !state.child_turns.is_empty(),
                background_pending: false,
                summary: None,
            },
        });
    }
    events
}

// -- subagents ----------------------------------------------------------------------

fn subagent_snapshot(shared: &Shared, child: &Child, now: Timestamp) -> Subagent {
    Subagent {
        id: child.node_id,
        thread_id: child.parent.thread_id,
        run_id: Some(child.parent.run_id),
        parent_node_id: child.parent.root_node_id,
        origin: SubagentOrigin::ProviderNative,
        created_by: Actor::Agent,
        driver: DRIVER,
        provider_instance_id: shared.instance.instance_id.clone(),
        provider_thread_id: Some(child.provider_thread_id),
        child_thread_id: None,
        native_task_ref: Some(ProviderRef::strong(DRIVER, &child.native)),
        prompt: child.prompt.clone(),
        title: child.title.clone(),
        model: child.model.clone(),
        status: child.status,
        progress: None,
        result: child.result.clone(),
        completion_wake: None,
        completion_delivery: None,
        started_at: Some(child.started_at),
        completed_at: child.status.is_terminal().then_some(now),
        updated_at: now,
    }
}

struct ChildSpec<'a> {
    native: &'a str,
    prompt: Option<&'a str>,
    title: Option<&'a str>,
    model: Option<&'a str>,
}

/// Registers a subagent thread the first time it is named: its provider
/// thread (owned by the subagent's node) and its subagent row.
fn register_child(
    state: &mut State,
    shared: &Shared,
    spec: ChildSpec<'_>,
    turn: &str,
    now: Timestamp,
) -> Vec<AdapterEvent> {
    if spec.native.is_empty() || state.children.contains_key(spec.native) {
        return Vec::new();
    }
    let Some(open) = state.turns.get(turn) else {
        return Vec::new();
    };
    let Some(parent) = open.run else {
        return Vec::new();
    };
    let pt = open.provider_thread_id.0;
    let child = Child {
        native: spec.native.to_owned(),
        provider_thread_id: shared.bind(pt, EntityKind::ProviderThread, spec.native),
        node_id: shared.bind(pt, EntityKind::Subagent, spec.native),
        parent,
        prompt: spec.prompt.or(spec.title).unwrap_or_default().to_owned(),
        title: spec.title.map(str::to_owned),
        model: spec.model.map(str::to_owned),
        status: SubagentStatus::Running,
        result: None,
        started_at: now,
        turns: 0,
    };
    let mut row = shared.thread_row(
        child.provider_thread_id,
        spec.native,
        Some(child.node_id),
        None,
    );
    row.status = ProviderThreadStatus::Active;
    let events = vec![
        AdapterEvent::ProviderThreadUpdated {
            thread: row,
            retained_native_turn_ids: None,
        },
        AdapterEvent::SubagentUpdated {
            subagent: subagent_snapshot(shared, &child, now),
        },
    ];
    state.children.insert(spec.native.to_owned(), child);
    events
}

fn set_child_status(
    state: &mut State,
    shared: &Shared,
    native: &str,
    status: SubagentStatus,
    result: Option<String>,
    now: Timestamp,
) -> Vec<AdapterEvent> {
    let Some(child) = state.children.get_mut(native) else {
        return Vec::new();
    };
    if result.is_some() {
        child.result = result;
    }
    if child.status == status {
        return Vec::new();
    }
    child.status = status;
    vec![AdapterEvent::SubagentUpdated {
        subagent: subagent_snapshot(shared, child, now),
    }]
}

/// Multi-agent v2: `subAgentActivity` items name the subagent's thread when
/// it starts and when it ends.
fn subagent_activity(
    state: &mut State,
    shared: &Shared,
    item: &Value,
    turn: &str,
    now: Timestamp,
) -> Vec<AdapterEvent> {
    let child = param(item, "agentThreadId").to_owned();
    let path = item.get("agentPath").and_then(Value::as_str);
    let mut events = register_child(
        state,
        shared,
        ChildSpec {
            native: &child,
            prompt: None,
            title: path,
            model: None,
        },
        turn,
        now,
    );
    let status = match param(item, "kind") {
        "completed" => Some(SubagentStatus::Completed),
        "interrupted" => Some(SubagentStatus::Interrupted),
        "failed" | "errored" => Some(SubagentStatus::Failed),
        _ => None,
    };
    if let Some(status) = status {
        events.extend(set_child_status(state, shared, &child, status, None, now));
    }
    offer_all(state, events)
}

fn agent_status(status: &str) -> SubagentStatus {
    match status {
        "pendingInit" => SubagentStatus::Pending,
        "completed" | "shutdown" => SubagentStatus::Completed,
        "interrupted" => SubagentStatus::Interrupted,
        "errored" | "notFound" => SubagentStatus::Failed,
        _ => SubagentStatus::Running,
    }
}

/// Multi-agent v1 (`spawnAgent`, `wait`, …): the receivers are subagent
/// threads and `agentsStates` their status.
fn collab_item(
    state: &mut State,
    shared: &Shared,
    item: &Value,
    turn: &str,
    now: Timestamp,
) -> Vec<AdapterEvent> {
    let prompt = item.get("prompt").and_then(Value::as_str);
    let model = item.get("model").and_then(Value::as_str);
    let mut events = Vec::new();
    for receiver in item
        .get("receiverThreadIds")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        events.extend(register_child(
            state,
            shared,
            ChildSpec {
                native: receiver,
                prompt,
                title: prompt,
                model,
            },
            turn,
            now,
        ));
    }
    if let Some(states) = item.get("agentsStates").and_then(Value::as_object) {
        let mut states = states.iter().collect::<Vec<_>>();
        states.sort_by(|left, right| left.0.cmp(right.0));
        for (thread, agent) in states {
            let status = agent_status(param(agent, "status"));
            let message = agent
                .get("message")
                .and_then(Value::as_str)
                .filter(|message| !message.is_empty() && status.is_terminal())
                .map(str::to_owned);
            events.extend(set_child_status(
                state, shared, thread, status, message, now,
            ));
        }
    }
    offer_all(state, events)
}

fn child_turn_started(
    state: &mut State,
    shared: &Shared,
    thread: &str,
    native: &str,
) -> Vec<AdapterEvent> {
    if state.child_turns.contains_key(native) {
        return Vec::new();
    }
    let now = shared.now();
    let Some(child) = state.children.get_mut(thread) else {
        return Vec::new();
    };
    child.turns += 1;
    let reference = ProviderTurnRef {
        provider_turn_id: shared.bind(child.provider_thread_id.0, EntityKind::ProviderTurn, native),
        native: NativeRef::strong(native),
        ordinal: child.turns,
    };
    let turn = ProviderTurn {
        id: reference.provider_turn_id,
        provider_thread_id: child.provider_thread_id,
        node_id: child.node_id,
        run_attempt_id: None,
        native_turn_ref: Some(reference.native.to_provider_ref(DRIVER)),
        ordinal: reference.ordinal,
        status: ProviderTurnStatus::Running,
        started_at: Some(now),
        completed_at: None,
        token_usage: None,
        turn_token_usage: None,
    };
    state.child_turns.insert(
        native.to_owned(),
        ChildTurn {
            child: thread.to_owned(),
            reference,
            started_at: now,
        },
    );
    let mut events = vec![AdapterEvent::ProviderTurnUpdated { turn }];
    events.extend(set_child_status(
        state,
        shared,
        thread,
        SubagentStatus::Running,
        None,
        now,
    ));
    offer_all(state, events)
}

/// A child turn's terminal names its own provider turn; it never completes
/// the parent run.
fn child_turn_completed(
    state: &mut State,
    shared: &Shared,
    native: &str,
    turn: &Value,
) -> Vec<AdapterEvent> {
    let now = shared.now();
    let Some(child_turn) = state.child_turns.remove(native) else {
        return Vec::new();
    };
    let mut events = state.coalescer.flush();
    let Some(child) = state.children.get_mut(&child_turn.child) else {
        return events;
    };
    if let Some(text) = turn
        .get("items")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .rev()
        .find(|item| item.get("type").and_then(Value::as_str) == Some("agentMessage"))
        .and_then(|item| item.get("text").and_then(Value::as_str))
    {
        child.result = Some(text.to_owned());
    }
    let (outcome, status) = match turn.get("status").and_then(Value::as_str) {
        Some("completed") => (TurnOutcome::Completed, ProviderTurnStatus::Completed),
        Some("interrupted") => (TurnOutcome::Interrupted, ProviderTurnStatus::Interrupted),
        _ => (
            TurnOutcome::Failed {
                failure: protocol::classify(turn.get("error").unwrap_or(&Value::Null), None),
            },
            ProviderTurnStatus::Failed,
        ),
    };
    events.push(AdapterEvent::ProviderTurnUpdated {
        turn: ProviderTurn {
            id: child_turn.reference.provider_turn_id,
            provider_thread_id: child.provider_thread_id,
            node_id: child.node_id,
            run_attempt_id: None,
            native_turn_ref: Some(child_turn.reference.native.to_provider_ref(DRIVER)),
            ordinal: child_turn.reference.ordinal,
            status,
            started_at: Some(child_turn.started_at),
            completed_at: Some(now),
            token_usage: None,
            turn_token_usage: None,
        },
    });
    events.push(AdapterEvent::TurnTerminal {
        terminal: TurnTerminal {
            run: child.parent,
            provider_turn: Some(child_turn.reference.clone()),
            outcome,
            thread_disposition: ThreadDisposition::Reusable,
            child_turns_continue: false,
            background_pending: false,
            summary: None,
        },
    });
    events
}

// -- server requests ----------------------------------------------------------------

fn server_request(
    shared: &Shared,
    method: &str,
    id: &Value,
    frame: &Value,
) -> (Vec<AdapterEvent>, Option<ServerAction>) {
    let params = frame.get("params").cloned().unwrap_or(Value::Null);
    match method {
        "item/commandExecution/requestApproval"
        | "item/fileChange/requestApproval"
        | "item/permissions/requestApproval" => approval(shared, method, id, &params),
        "item/tool/requestUserInput" => question(shared, id, &params),
        "mcpServer/elicitation/request" => {
            // Elicitation is a declared gap; declining keeps the turn going.
            shared
                .conn
                .raw_log(FrameDirection::Inbound, &frame.to_string());
            (
                Vec::new(),
                Some(ServerAction::Respond {
                    id: id.clone(),
                    result: json!({ "action": "decline" }),
                }),
            )
        }
        _ => {
            shared
                .conn
                .raw_log(FrameDirection::Inbound, &frame.to_string());
            (
                Vec::new(),
                Some(ServerAction::Reject {
                    id: id.clone(),
                    message: format!("Waku does not handle {method}"),
                }),
            )
        }
    }
}

/// The stable key a request's app id binds to: the item it is about (and
/// its approval id), not the per-connection JSON-RPC id, which repeats
/// across sessions.
fn native_request_key(params: &Value, id: &Value) -> String {
    let item = param(params, "itemId");
    let approval = param(params, "approvalId");
    match (item.is_empty(), approval.is_empty()) {
        (false, true) => item.to_owned(),
        (false, false) => format!("{item}:{approval}"),
        _ => rpc_id_string(id),
    }
}

fn approval(
    shared: &Shared,
    method: &str,
    id: &Value,
    params: &Value,
) -> (Vec<AdapterEvent>, Option<ServerAction>) {
    let now = shared.now();
    let mut guard = shared.state.lock();
    let state = &mut *guard;
    let decisions = if method == "item/permissions/requestApproval" {
        protocol::permission_decisions()
    } else {
        protocol::approval_decisions(params)
    };
    let Some(cx) = RowContext::of(state, param(params, "turnId")) else {
        // No run to ask in: refuse rather than leave Codex waiting.
        let decline = decisions
            .iter()
            .find(|decision| !decision.allow)
            .cloned()
            .unwrap_or(Decision {
                id: "decline".into(),
                label: String::new(),
                allow: false,
                value: json!("decline"),
            });
        return (
            Vec::new(),
            Some(ServerAction::Respond {
                id: id.clone(),
                result: protocol::approval_reply(method, params, &decline),
            }),
        );
    };
    let (kind, prompt) = match method {
        "item/commandExecution/requestApproval" => (
            RuntimeRequestKind::CommandApproval,
            params
                .get("command")
                .and_then(Value::as_str)
                .map(str::to_owned),
        ),
        "item/fileChange/requestApproval" => (
            RuntimeRequestKind::FileChangeApproval,
            Some(
                params
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or("Apply the proposed file changes")
                    .to_owned(),
            ),
        ),
        _ => (
            RuntimeRequestKind::Permission,
            Some(
                params
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or("Grant additional permissions")
                    .to_owned(),
            ),
        ),
    };
    let key = native_request_key(params, id);
    let request_id: RuntimeRequestId =
        shared.bind(cx.provider_thread_id.0, EntityKind::Request, &key);
    let options = decisions
        .iter()
        .map(|decision| PermissionOption {
            id: decision.id.clone(),
            label: decision.label.clone(),
            allow: decision.allow,
        })
        .collect::<Vec<_>>();
    let request = RuntimeRequest {
        id: request_id,
        thread_id: cx.run.thread_id,
        node_id: cx.node,
        provider_turn_id: Some(cx.provider_turn_id),
        native_request_ref: Some(ProviderRef::strong(DRIVER, rpc_id_string(id))),
        kind,
        status: RequestStatus::Pending,
        response_capability: ResponseCapability::Live {
            provider_session_id: shared.session_id,
        },
        options: options.clone(),
        questions: Vec::new(),
        response: None,
        created_at: now,
        resolved_at: None,
    };
    let events = vec![
        AdapterEvent::TurnItemUpdated {
            item: cx.row(
                derived::request_item(request_id),
                &key,
                None,
                now,
                now,
                ItemStatus::Waiting,
                None,
                TurnItemKind::ApprovalRequest {
                    request_id,
                    request_kind: kind,
                    prompt,
                    app_name: None,
                    options,
                },
            ),
        },
        AdapterEvent::RuntimeRequestUpdated {
            request: request.clone(),
        },
    ];
    state.requests.insert(
        request_id,
        PendingRequest {
            rpc_id: id.clone(),
            method: method.to_owned(),
            params: params.clone(),
            decisions,
            request,
            question_ids: Vec::new(),
            by_message: false,
        },
    );
    (offer_all(state, events), None)
}

fn questions_of(params: &Value) -> Vec<UserInputQuestion> {
    params
        .get("questions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
        .filter_map(|(index, question)| {
            let text = question.get("question").and_then(Value::as_str)?.trim();
            if text.is_empty() {
                return None;
            }
            Some(UserInputQuestion {
                id: question
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("question-{index}")),
                header: question
                    .get("header")
                    .and_then(Value::as_str)
                    .filter(|header| !header.trim().is_empty())
                    .unwrap_or("Question")
                    .to_owned(),
                question: text.to_owned(),
                options: question
                    .get("options")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|option| {
                        let label = option.get("label").and_then(Value::as_str)?.trim();
                        (!label.is_empty()).then(|| UserInputOption {
                            label: label.to_owned(),
                            description: option
                                .get("description")
                                .and_then(Value::as_str)
                                .map(str::trim)
                                .filter(|description| !description.is_empty())
                                .map(str::to_owned),
                        })
                    })
                    .collect(),
                // Codex's request schema is single-select.
                multi_select: false,
            })
        })
        .collect()
}

fn question(
    shared: &Shared,
    id: &Value,
    params: &Value,
) -> (Vec<AdapterEvent>, Option<ServerAction>) {
    let now = shared.now();
    let mut guard = shared.state.lock();
    let state = &mut *guard;
    let questions = questions_of(params);
    let question_ids = questions
        .iter()
        .map(|question| question.id.clone())
        .collect::<Vec<_>>();
    let cx = RowContext::of(state, param(params, "turnId")).filter(|_| !questions.is_empty());
    let Some(cx) = cx else {
        return (
            Vec::new(),
            Some(ServerAction::Respond {
                id: id.clone(),
                result: json!({
                    "answers": question_ids
                        .iter()
                        .map(|id| (id.clone(), json!({ "answers": [] })))
                        .collect::<serde_json::Map<_, _>>()
                }),
            }),
        );
    };
    let key = native_request_key(params, id);
    let request_id: RuntimeRequestId =
        shared.bind(cx.provider_thread_id.0, EntityKind::Request, &key);
    let request = RuntimeRequest {
        id: request_id,
        thread_id: cx.run.thread_id,
        node_id: cx.node,
        provider_turn_id: Some(cx.provider_turn_id),
        native_request_ref: Some(ProviderRef::strong(DRIVER, rpc_id_string(id))),
        kind: RuntimeRequestKind::UserInput,
        status: RequestStatus::Pending,
        response_capability: ResponseCapability::Live {
            provider_session_id: shared.session_id,
        },
        options: Vec::new(),
        questions,
        response: None,
        created_at: now,
        resolved_at: None,
    };
    state.requests.insert(
        request_id,
        PendingRequest {
            rpc_id: id.clone(),
            method: "item/tool/requestUserInput".to_owned(),
            params: params.clone(),
            decisions: Vec::new(),
            request: request.clone(),
            question_ids,
            by_message: false,
        },
    );
    let events = state
        .coalescer
        .offer(AdapterEvent::RuntimeRequestUpdated { request });
    (events, None)
}

// -- snapshots ----------------------------------------------------------------------

/// A `thread/read` result as a thread snapshot, native ids bound under the
/// provider thread.
pub(super) fn snapshot(
    shared: &Shared,
    thread: &ProviderThreadRef,
    result: &Value,
) -> ThreadSnapshot {
    let native = str_at(result, "/thread/id")
        .unwrap_or(&thread.native.id)
        .to_owned();
    let title = str_at(result, "/thread/name").map(str::to_owned);
    let pt = thread.provider_thread_id;
    let now = shared.now();
    let mut turns = Vec::new();
    let mut messages = Vec::new();
    for (index, turn) in result
        .pointer("/thread/turns")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
    {
        let Some(turn_id) = turn.get("id").and_then(Value::as_str) else {
            continue;
        };
        let status = match turn.get("status").and_then(Value::as_str) {
            Some("interrupted") => ProviderTurnStatus::Interrupted,
            Some("failed") => ProviderTurnStatus::Failed,
            Some("inProgress") => ProviderTurnStatus::Running,
            _ => ProviderTurnStatus::Completed,
        };
        let id: ProviderTurnId = shared.bind(pt.0, EntityKind::ProviderTurn, turn_id);
        let seconds = |key: &str| {
            turn.get(key)
                .and_then(Value::as_i64)
                .map(|secs| secs * 1000)
        };
        turns.push(ProviderTurn {
            id,
            provider_thread_id: pt,
            node_id: shared.bind(pt.0, EntityKind::Node, &format!("turn:{turn_id}")),
            run_attempt_id: None,
            native_turn_ref: Some(ProviderRef::strong(DRIVER, turn_id)),
            ordinal: index as u32 + 1,
            status,
            started_at: seconds("startedAt"),
            completed_at: seconds("completedAt"),
            token_usage: None,
            turn_token_usage: None,
        });
        for item in turn
            .get("items")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(item_id) = item.get("id").and_then(Value::as_str) else {
                continue;
            };
            let (role, text) = match item.get("type").and_then(Value::as_str) {
                Some("userMessage") => (
                    MessageRole::User,
                    item.get("content")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(|part| part.get("text").and_then(Value::as_str))
                        .collect::<Vec<_>>()
                        .join("\n"),
                ),
                Some("agentMessage") => (
                    MessageRole::Assistant,
                    item.get("text")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                ),
                _ => continue,
            };
            messages.push(Message {
                id: shared.bind(pt.0, EntityKind::Message, item_id),
                thread_id: shared.thread_id,
                run_id: None,
                node_id: None,
                role,
                created_by: if role == MessageRole::User {
                    Actor::User
                } else {
                    Actor::Agent
                },
                creation_source: CreationSource::Provider,
                text,
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
    ThreadSnapshot {
        thread: shared.thread_row(pt, &native, None, title),
        turns,
        messages,
        pending_requests: Vec::new(),
    }
}

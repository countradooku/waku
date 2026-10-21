//! The Codex app-server driver (`codex app-server`), ported to `DriverV2`
//! (RFC 0002, #46).
//!
//! One actor per session owns one app-server process reached through
//! `ctx.transports`: the single stdin writer and JSON-RPC id counter live in
//! [`protocol::Connection`], the read loop in [`normalize`] turns every
//! native frame into `AdapterEvent` entity snapshots, and the `DriverSession`
//! methods below send requests and await their responses. Everything a
//! session does — threads, turns, steering, approvals, questions, goals,
//! rollback (`thread/revert`), forks (`thread/fork`), snapshots
//! (`thread/read`) and background terminals — runs on that one live
//! connection; no second app-server is started for a live session.
//!
//! [`legacy`] serves the session behind the old `DriverControl` interface
//! through `V1Bridge` until the orchestrator cutover (D-9), and [`title`]
//! keeps the one-shot title generation the orchestrator's `TitlePort` uses.

mod legacy;
mod normalize;
mod protocol;
pub mod script;
mod title;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};
use waku_protocol::model::ProviderResumeCursor;
use waku_protocol::orchestration::{self as orch, *};

use self::normalize::{ServerAction, State};
use self::protocol::Connection;
use crate::driver::v2::coalescer::DeltaCoalescer;
use crate::driver::v2::{RuntimeResponse, *};

pub(crate) use self::legacy::start_local;
pub(crate) use self::title::generate_codex_title;

/// `ProviderKind::Codex.id()`.
pub const CODEX_PROVIDER: &str = "codex";
/// The replay protocol name of the app-server's stdio JSON-RPC.
pub const CODEX_PROTOCOL: &str = "codex.app-server";
/// The CLI version the committed fixtures were recorded or derived from.
pub const CODEX_FIXTURE_VERSION: &str = "0.156.1";

const DRIVER: orch::DriverKind = orch::DriverKind::Codex;

/// An instance of the Codex provider (tests, the recorder).
pub fn codex_instance() -> InstanceConfig {
    let mut cfg = InstanceConfig::new(CODEX_PROVIDER, orch::DriverKind::Codex);
    cfg.binary = "codex".into();
    cfg.cli_version = Some(CODEX_FIXTURE_VERSION.to_owned());
    cfg
}

/// What the ported Codex driver does (RFC 0002's matrix, Codex column, with
/// the amendments recorded there for #46).
pub fn codex_capabilities(cfg: &InstanceConfig) -> Capabilities {
    let mut caps = Capabilities::default();
    caps.sessions.model_switch_in_session = true;
    caps.sessions.provider_switching_via_handoff = true;
    caps.threads.create_empty_thread = true;
    caps.threads.read_snapshot = true;
    caps.threads.rollback = true;
    caps.threads.fork = true;
    caps.threads.fork_from_turn = true;
    caps.threads.native_thread_id = true;
    caps.turns.native_turn_id = true;
    caps.turns.emits_turn_started = true;
    caps.turns.emits_turn_completed = true;
    caps.turns.interrupt = true;
    caps.turns.active_steering = true;
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
    caps.tools.mcp_tools = true;
    caps.approvals.command = true;
    caps.approvals.file_read = true;
    caps.approvals.file_change = true;
    caps.approvals.apply_patch = true;
    caps.approvals.native_request_ids = true;
    caps.approvals.callbacks_are_live_only = true;
    caps.planning.emits_plan_updated = true;
    caps.planning.emits_todo_list = true;
    caps.planning.structured_questions = true;
    caps.subagents.supported = true;
    caps.subagents.emits_lifecycle = true;
    caps.subagents.exposes_thread_ids = true;
    caps.context.accepts_developer_context = true;
    caps.context.accepts_synthetic_user_context = true;
    caps.context.can_consume_handoff_summaries = true;
    caps.context.delta_handoff = true;
    caps.context.full_thread_handoff = true;
    caps.checkpointing.app_can_checkpoint_filesystem = true;
    caps.checkpointing.nested_scopes = true;
    caps.checkpointing.provider_can_rollback_conversation = true;
    caps.checkpointing.provider_can_read_conversation_snapshot = true;
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
    caps.extras.goals = true;
    caps.extras.background_work = true;
    caps.extras.account_usage_stream = true;
    caps.extras.computer_use = cfg.computer_use_enabled;
    caps.extras.model_discovery = true;
    caps
}

/// The Codex driver family. Holds no per-session state; `legacy` is set only
/// by the `V1Bridge` wrapper, which needs the raw frames for the old
/// desktop events.
#[derive(Default)]
pub struct CodexDriver {
    legacy: Option<Arc<legacy::Sidecar>>,
}

impl CodexDriver {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl DriverV2 for CodexDriver {
    fn kind(&self) -> DriverFamily {
        DriverFamily::Codex
    }

    fn capabilities(&self, cfg: &InstanceConfig) -> Capabilities {
        codex_capabilities(cfg)
    }

    fn plan_selection_transition(
        &self,
        _session_caps: &Capabilities,
        from: &Selection,
        to: &Selection,
    ) -> TransitionPlan {
        let mut plans = Vec::new();
        // Model, effort, tier and window ride on every `turn/start`; Codex has
        // no agent presets, so a preset change is ignored on the next turn.
        if from.model != to.model
            || from.reasoning_effort != to.reasoning_effort
            || from.service_tier != to.service_tier
            || from.context_window != to.context_window
            || from.agent_preset != to.agent_preset
        {
            plans.push(TransitionPlan::ApplyOnNextTurn);
        }
        // The permission profile the thread was opened with deserves a fresh
        // session (today's `apply_options == false`).
        if from.runtime_mode != to.runtime_mode {
            plans.push(TransitionPlan::RestartSession);
        }
        if from.interaction_mode != to.interaction_mode {
            plans.push(TransitionPlan::Reject {
                reason: "Codex plan mode arrives with #68".to_owned(),
            });
        }
        TransitionPlan::combine(plans)
    }

    async fn open_session(&self, ctx: SessionContext) -> DriverResult<OpenedSession> {
        let capabilities = self.capabilities(&ctx.instance);
        let computer_use = if ctx.instance.computer_use_enabled {
            Some(Arc::new(legacy::ComputerUse::load().map_err(|error| {
                DriverError::Io(std::io::Error::other(error.to_string()))
            })?))
        } else {
            None
        };
        let mut args = vec!["app-server".to_owned(), "--stdio".to_owned()];
        let mut env = ctx.instance.env.clone();
        if let Some(home) = &ctx.instance.provider_home {
            env.push(("CODEX_HOME".to_owned(), home.display().to_string()));
        }
        if let Some(computer_use) = &computer_use {
            computer_use.configure(&mut args, &mut env);
        }
        let transport = ctx
            .transports
            .spawn_stdio(
                "main",
                ProcessSpec {
                    program: ctx.instance.binary.clone(),
                    args,
                    env,
                    cwd: ctx.cwd.clone(),
                },
            )
            .await?;
        let (events, receiver) = adapter_channel();
        let now = ctx.clock.now();
        let shared = Arc::new(Shared {
            conn: Connection::new("main", transport.stdin, ctx.raw_log.clone()),
            session_id: ctx.provider_session_id,
            thread_id: ctx.thread_id,
            instance: ctx.instance.clone(),
            ids: ctx.ids.clone(),
            clock: ctx.clock.clone(),
            cwd: ctx.cwd.display().to_string(),
            capabilities: capabilities.clone(),
            selection: Mutex::new(ctx.selection.clone()),
            state: Mutex::new(State::new(DeltaCoalescer::new(ctx.clock.clone()))),
            events: Mutex::new(Some(events)),
            legacy: self.legacy.clone(),
            computer_use,
            created_at: now,
        });
        if let Some(legacy) = &self.legacy {
            legacy.attach(shared.clone());
        }
        tokio::spawn(normalize::read_loop(
            shared.clone(),
            transport.stdout,
            transport.exit,
        ));
        tokio::spawn(stderr_loop(shared.clone(), transport.stderr));
        shared
            .conn
            .request("initialize", protocol::initialize_params())
            .await?;
        shared.conn.notify("initialized", json!({})).await?;
        if let Some(computer_use) = &shared.computer_use {
            // Waku's bundled skill registers through Codex's discoverable
            // roots, so it loads like Codex's own skills.
            shared
                .conn
                .request(
                    "skills/extraRoots/set",
                    json!({ "extraRoots": [computer_use.skill_root()] }),
                )
                .await?;
        }
        Ok(OpenedSession {
            session: Arc::new(CodexSession { shared }),
            events: receiver,
            capabilities,
        })
    }
}

async fn stderr_loop(shared: Arc<Shared>, stderr: Box<dyn tokio::io::AsyncRead + Send + Unpin>) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        shared.conn.raw_log(FrameDirection::Note, &line);
        if let Some(legacy) = &shared.legacy {
            legacy.stderr(&line);
        }
    }
}

/// What a session's tasks share. The state lock is never held across an
/// await.
struct Shared {
    conn: Connection,
    session_id: ProviderSessionId,
    thread_id: ThreadId,
    instance: InstanceConfig,
    ids: Arc<dyn IdAllocator>,
    clock: Arc<dyn Clock>,
    cwd: String,
    capabilities: Capabilities,
    selection: Mutex<Selection>,
    state: Mutex<State>,
    events: Mutex<Option<AdapterEventSender>>,
    legacy: Option<Arc<legacy::Sidecar>>,
    computer_use: Option<Arc<legacy::ComputerUse>>,
    created_at: Timestamp,
}

impl Shared {
    fn now(&self) -> Timestamp {
        self.clock.now()
    }

    fn bind<T: From<uuid::Uuid>>(&self, scope: uuid::Uuid, kind: EntityKind, native: &str) -> T {
        T::from(self.ids.bind(scope, kind, native))
    }

    /// Sends events in order. The bounded channel slows the reader down when
    /// the sink lags (RFC 0002 rule 6).
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

    /// Passes events through the delta coalescer, then sends them.
    async fn emit_coalesced(&self, events: Vec<AdapterEvent>) {
        let events = {
            let mut state = self.state.lock();
            events
                .into_iter()
                .flat_map(|event| state.coalescer.offer(event))
                .collect::<Vec<_>>()
        };
        self.emit(events).await;
    }

    /// Runs a server-request side effect the read loop decided on.
    async fn act(&self, action: ServerAction) {
        let result = match action {
            ServerAction::Respond { id, result } => self.conn.respond(&id, result).await,
            ServerAction::Reject { id, message } => {
                self.conn.respond_error(&id, -32601, &message).await
            }
        };
        if let Err(error) = result {
            self.conn
                .raw_log(FrameDirection::Note, &format!("reply failed: {error}"));
        }
    }

    /// A provider thread row for a thread this session knows natively. Only
    /// child (subagent) threads, snapshots and the `V1Bridge` use it: the
    /// orchestrator owns the app-side fields of a root row.
    fn thread_row(
        &self,
        id: ProviderThreadId,
        native: &str,
        owner: Option<NodeId>,
        title: Option<String>,
    ) -> ProviderThread {
        let now = self.now();
        let cursor = serde_json::to_string(&ProviderResumeCursor::Codex {
            thread_id: native.to_owned(),
        })
        .ok()
        .map(JsonText);
        ProviderThread {
            id,
            driver: DRIVER,
            provider_instance_id: self.instance.instance_id.clone(),
            provider_session_id: Some(self.session_id),
            app_thread_id: Some(self.thread_id),
            owner_node_id: owner,
            native_thread_ref: Some(ProviderRef::strong(DRIVER, native)),
            native_conversation_head_ref: None,
            status: ProviderThreadStatus::Idle,
            first_run_ordinal: None,
            last_run_ordinal: None,
            handoff_ids: Vec::new(),
            forked_from: None,
            pending_background_tasks: Vec::new(),
            context_usage: None,
            goal: None,
            native_metadata: Some(NativeMetadata {
                title,
                updated_at: Some(now),
                resume_cursor: cursor,
            }),
            created_at: now,
            updated_at: now,
        }
    }

    /// Binds this session's root thread and, behind the bridge, reports it.
    async fn bind_root(
        &self,
        provider_thread_id: ProviderThreadId,
        native: &str,
        title: Option<String>,
    ) {
        self.state.lock().bind_root(provider_thread_id, native);
        if self.legacy.is_some() {
            // The bridge turns the cursor into `Connected` and the name
            // into the auto title. In orchestrator mode the manager writes
            // the row itself (it owns its app-side fields).
            let row = self.thread_row(provider_thread_id, native, None, title);
            self.emit_coalesced(vec![AdapterEvent::ProviderThreadUpdated {
                thread: row,
                retained_native_turn_ids: None,
            }])
            .await;
        }
    }

    fn root_native(&self) -> DriverResult<String> {
        self.state
            .lock()
            .root
            .as_ref()
            .map(|root| root.native.clone())
            .ok_or(DriverError::NotReady(NotReadyReason::ThreadMissing))
    }
}

const RESUME_RETRIES: u32 = 8;

fn is_writer_conflict(error: &protocol::RpcError) -> bool {
    let message = error.message.to_ascii_lowercase();
    message.contains("active writer") || message.contains("already loaded")
}

fn thread_name(result: &Value) -> Option<String> {
    result
        .pointer("/thread/name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
}

fn thread_native_id(result: &Value) -> DriverResult<String> {
    result
        .pointer("/thread/id")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| DriverError::Protocol {
            reason: ProtocolReason::UnexpectedFrame,
            detail: format!("no thread id in {result}"),
        })
}

fn cursor_for(native: &str) -> ProviderResumeCursor {
    ProviderResumeCursor::Codex {
        thread_id: native.to_owned(),
    }
}

/// One live Codex session.
pub struct CodexSession {
    shared: Arc<Shared>,
}

impl CodexSession {
    /// The native id of the root turn running now, for the bridge's Stop,
    /// which also stops provider-initiated (`/goal`) turns.
    async fn interrupt_active(&self) -> DriverResult<()> {
        let target = {
            let state = self.shared.state.lock();
            state
                .root
                .as_ref()
                .map(|root| root.native.clone())
                .zip(state.active_root_turn())
        };
        if let Some((thread, turn)) = target {
            self.shared
                .conn
                .request(
                    "turn/interrupt",
                    json!({ "threadId": thread, "turnId": turn }),
                )
                .await?;
        }
        Ok(())
    }

    /// `thread/backgroundTerminals/list`, every page.
    async fn background_terminals(&self) -> DriverResult<Vec<Value>> {
        let thread = self.shared.root_native()?;
        let mut terminals = Vec::new();
        let mut cursor = Value::Null;
        loop {
            let page = self
                .shared
                .conn
                .request(
                    "thread/backgroundTerminals/list",
                    json!({ "threadId": thread, "cursor": cursor, "limit": 100 }),
                )
                .await?;
            terminals.extend(
                page.get("data")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default(),
            );
            match page.get("nextCursor").filter(|next| !next.is_null()) {
                Some(next) => cursor = next.clone(),
                None => return Ok(terminals),
            }
        }
    }

    /// Persists a generated title (`thread/name/set`).
    async fn set_thread_name(&self, name: &str) -> DriverResult<()> {
        let thread = self.shared.root_native()?;
        self.shared
            .conn
            .request(
                "thread/name/set",
                json!({ "threadId": thread, "name": name }),
            )
            .await
            .map(|_| ())
    }

    async fn goal_updated(&self, goal: Option<ThreadGoal>) {
        self.shared.state.lock().goal = goal.clone();
        self.shared
            .emit_coalesced(vec![AdapterEvent::GoalUpdated { goal }])
            .await;
    }

    /// Native turns, newest first, until `count` are collected: the oldest
    /// of them is the first turn a revert drops.
    async fn nth_newest_turn(&self, thread: &str, count: u32) -> DriverResult<Option<String>> {
        let mut remaining = count;
        let mut before = None;
        let mut cursor = Value::Null;
        let mut seen = std::collections::HashSet::new();
        while remaining > 0 {
            if !seen.insert(cursor.to_string()) {
                return Err(DriverError::Protocol {
                    reason: ProtocolReason::UnexpectedFrame,
                    detail: "thread/turns/list repeated a cursor".to_owned(),
                });
            }
            let page = self
                .shared
                .conn
                .request(
                    "thread/turns/list",
                    json!({
                        "threadId": thread,
                        "cursor": cursor,
                        "limit": remaining.min(100),
                        "sortDirection": "desc",
                        "itemsView": "summary",
                    }),
                )
                .await?;
            for turn in page
                .get("data")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(id) = turn.get("id").and_then(Value::as_str) {
                    before = Some(id.to_owned());
                    remaining -= 1;
                    if remaining == 0 {
                        break;
                    }
                }
            }
            match page.get("nextCursor").filter(|next| !next.is_null()) {
                Some(next) => cursor = next.clone(),
                None => break,
            }
        }
        Ok(before)
    }
}

#[async_trait]
impl DriverSession for CodexSession {
    fn capabilities(&self) -> &Capabilities {
        &self.shared.capabilities
    }

    async fn ensure_thread(&self, req: EnsureThread) -> DriverResult<ProviderThreadRef> {
        *self.shared.selection.lock() = req.selection.clone();
        let result = self
            .shared
            .conn
            .request(
                "thread/start",
                protocol::thread_start_params(&self.shared.cwd, &req.selection),
            )
            .await?;
        let native = thread_native_id(&result)?;
        let provider_thread_id = match req.existing.as_ref() {
            Some(existing) => existing.provider_thread_id,
            None => self.shared.bind(
                self.shared.session_id.0,
                EntityKind::ProviderThread,
                &native,
            ),
        };
        self.shared
            .bind_root(provider_thread_id, &native, thread_name(&result))
            .await;
        // A thread this session started has no turns it has not seen.
        self.shared.state.lock().mark_history_complete();
        Ok(ProviderThreadRef {
            provider_thread_id,
            native: NativeRef::strong(&native),
            cursor: Some(cursor_for(&native)),
        })
    }

    async fn resume_thread(&self, req: ResumeThread) -> DriverResult<ProviderThreadRef> {
        *self.shared.selection.lock() = req.selection.clone();
        let mut params =
            protocol::thread_resume_params(&req.thread.native.id, &self.shared.cwd, &req.selection);
        // History is read page by page (`thread/turns/list`) when needed.
        params["excludeTurns"] = json!(true);
        let mut attempt = 0;
        let result = loop {
            match self
                .shared
                .conn
                .request_raw("thread/resume", params.clone())
                .await?
            {
                Ok(result) => break result,
                // A thread forked in another session's app-server stays
                // loaded there until that server lets go of it (`fork`
                // unsubscribes); give it a moment.
                Err(error) if is_writer_conflict(&error) && attempt < RESUME_RETRIES => {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                }
                Err(error) => return Err(protocol::rpc_failure("thread/resume", &error)),
            }
        };
        let native = thread_native_id(&result).unwrap_or_else(|_| req.thread.native.id.clone());
        self.shared
            .bind_root(req.thread.provider_thread_id, &native, thread_name(&result))
            .await;
        Ok(ProviderThreadRef {
            provider_thread_id: req.thread.provider_thread_id,
            native: NativeRef::strong(&native),
            cursor: Some(cursor_for(&native)),
        })
    }

    async fn start_turn(&self, req: TurnStart) -> DriverResult<TurnAccepted> {
        self.shared
            .state
            .lock()
            .begin_start(&req)
            .map_err(DriverError::NotReady)?;
        *self.shared.selection.lock() = req.selection.clone();
        let params = protocol::turn_start_params(
            &req.thread.native.id,
            &req.message.text,
            &req.selection,
            &req.app_context,
        );
        let result = match self.shared.conn.request("turn/start", params).await {
            Ok(result) => result,
            Err(error) => {
                self.shared.state.lock().abort_start(&req.run);
                return Err(error);
            }
        };
        let native = result
            .pointer("/turn/id")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| DriverError::Protocol {
                reason: ProtocolReason::UnexpectedFrame,
                detail: format!("turn/start: no turn id in {result}"),
            })?;
        // The read loop opened the turn when it saw this response; an
        // interrupt that arrived before then is delivered now.
        let cancel = self.shared.state.lock().take_cancel(&native);
        if cancel {
            self.shared
                .conn
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
        let turn = self
            .shared
            .state
            .lock()
            .native_turn_of(req.provider_turn.as_ref(), &req.run);
        let Some(turn) = turn else {
            return Err(DriverError::SteerRejected {
                reason: SteerRejectReason::NoActiveTurn,
            });
        };
        self.shared
            .conn
            .request(
                "turn/steer",
                json!({
                    "threadId": req.thread.native.id,
                    "expectedTurnId": turn,
                    "input": protocol::text_input(&req.message.text),
                }),
            )
            .await
            .map(|_| ())
    }

    async fn interrupt_turn(&self, req: Interrupt) -> DriverResult<()> {
        let turn = {
            let mut state = self.shared.state.lock();
            match state.native_turn_of(req.provider_turn.as_ref(), &req.run) {
                Some(turn) => Some(turn),
                None => {
                    // Before Codex named the turn: `start_turn` interrupts it
                    // as soon as the response arrives, and that turn's
                    // terminal is the run's one terminal.
                    state.cancel_start(&req.run);
                    None
                }
            }
        };
        if let Some(turn) = turn {
            // The session stays: Codex keeps the thread loaded and reusable.
            self.shared
                .conn
                .request(
                    "turn/interrupt",
                    json!({ "threadId": req.thread.native.id, "turnId": turn }),
                )
                .await?;
        }
        Ok(())
    }

    async fn respond(&self, req: RuntimeResponse) -> DriverResult<()> {
        let (id, result, resolved) = {
            let mut state = self.shared.state.lock();
            let now = self.shared.now();
            state.answer(&req, now)?
        };
        self.shared.conn.respond(&id, result).await?;
        self.shared
            .emit_coalesced(vec![AdapterEvent::RuntimeRequestUpdated {
                request: resolved,
            }])
            .await;
        Ok(())
    }

    async fn read_snapshot(&self, thread: &ProviderThreadRef) -> DriverResult<ThreadSnapshot> {
        let result = self
            .shared
            .conn
            .request(
                "thread/read",
                json!({ "threadId": thread.native.id, "includeTurns": true }),
            )
            .await?;
        Ok(normalize::snapshot(&self.shared, thread, &result))
    }

    async fn rollback(&self, req: Rollback) -> DriverResult<RollbackOutcome> {
        let target = match &req.target {
            RollbackTarget::ThreadStart => None,
            RollbackTarget::AfterProviderTurn(turn) => Some(turn.native.id.clone()),
        };
        // Codex counts native turns, `/goal` continuations included, while
        // the orchestrator counts its own runs: the turns this session saw
        // after the target decide, else the orchestrator's count.
        let count = self
            .shared
            .state
            .lock()
            .native_turns_after(target.as_deref())
            .unwrap_or(req.turns_to_remove);
        if count == 0 {
            return Ok(RollbackOutcome {
                thread: req.thread,
                replaced: false,
                snapshot: None,
                retained_native_turn_ids: None,
            });
        }
        let Some(before) = self.nth_newest_turn(&req.thread.native.id, count).await? else {
            return Ok(RollbackOutcome {
                thread: req.thread,
                replaced: false,
                snapshot: None,
                retained_native_turn_ids: None,
            });
        };
        let result = self
            .shared
            .conn
            .request(
                "thread/revert",
                json!({ "threadId": req.thread.native.id, "beforeTurnId": before }),
            )
            .await?;
        let retained = self.shared.state.lock().reverted(&before);
        let native = thread_native_id(&result).unwrap_or_else(|_| req.thread.native.id.clone());
        let replaced = native != req.thread.native.id;
        let thread = if replaced {
            ProviderThreadRef {
                provider_thread_id: self.shared.bind(
                    req.thread.provider_thread_id.0,
                    EntityKind::ProviderThread,
                    &native,
                ),
                native: NativeRef::strong(&native),
                cursor: Some(cursor_for(&native)),
            }
        } else {
            req.thread
        };
        Ok(RollbackOutcome {
            thread,
            replaced,
            snapshot: None,
            retained_native_turn_ids: retained,
        })
    }

    async fn fork(&self, req: ForkRequest) -> DriverResult<ProviderThreadRef> {
        let mut params = json!({
            "threadId": req.source.native.id,
            "cwd": self.shared.cwd,
            "excludeTurns": true,
            "deferGoalContinuation": true,
        });
        match &req.at {
            ForkPoint::Tip => {}
            // `lastTurnId` is inclusive: the fork keeps that turn and drops
            // every later one atomically.
            ForkPoint::AfterProviderTurn { turn, .. } => {
                params["lastTurnId"] = json!(turn.native.id);
            }
            ForkPoint::SubagentThread(native) => {
                params["threadId"] = json!(native.id);
            }
        }
        let result = self.shared.conn.request("thread/fork", params).await?;
        let native = thread_native_id(&result)?;
        // The fork's turns run in another session: this app-server lets go
        // of the new thread so that session can load it.
        if let Err(error) = self
            .shared
            .conn
            .request("thread/unsubscribe", json!({ "threadId": native }))
            .await
        {
            self.shared.conn.raw_log(
                FrameDirection::Note,
                &format!("thread/unsubscribe after fork failed: {error}"),
            );
        }
        Ok(ProviderThreadRef {
            provider_thread_id: self.shared.bind(
                req.source.provider_thread_id.0,
                EntityKind::ProviderThread,
                &native,
            ),
            native: NativeRef::strong(&native),
            cursor: Some(cursor_for(&native)),
        })
    }

    async fn goal(&self, op: GoalRequest) -> DriverResult<()> {
        let thread = self.shared.root_native()?;
        match op {
            GoalRequest::Get => {
                let result = self
                    .shared
                    .conn
                    .request("thread/goal/get", json!({ "threadId": thread }))
                    .await?;
                let goal = result.get("goal").and_then(protocol::parse_goal);
                self.goal_updated(goal).await;
            }
            GoalRequest::Clear => {
                self.shared
                    .conn
                    .request("thread/goal/clear", json!({ "threadId": thread }))
                    .await?;
                self.goal_updated(None).await;
            }
            GoalRequest::Set(goal) => {
                // A new objective starts with fresh accounting: clear the
                // old goal first. A status-only change keeps it.
                let replace = !goal.objective.is_empty()
                    && self
                        .shared
                        .state
                        .lock()
                        .goal
                        .as_ref()
                        .is_some_and(|current| current.objective != goal.objective);
                if replace {
                    self.shared
                        .conn
                        .request("thread/goal/clear", json!({ "threadId": thread }))
                        .await?;
                }
                let result = self
                    .shared
                    .conn
                    .request("thread/goal/set", protocol::goal_set_params(&thread, &goal))
                    .await?;
                if let Some(goal) = result.get("goal").and_then(protocol::parse_goal) {
                    self.goal_updated(Some(goal)).await;
                }
            }
        }
        Ok(())
    }

    async fn stop_background_work(&self, work: BackgroundWorkRef) -> DriverResult<()> {
        let thread = self.shared.root_native()?;
        let result = self
            .shared
            .conn
            .request(
                "thread/backgroundTerminals/terminate",
                json!({ "threadId": thread, "processId": work.task_id }),
            )
            .await?;
        if result.get("terminated").and_then(Value::as_bool) == Some(false) {
            return Err(DriverError::Provider(ProviderFailure::new(
                FailureClass::ProviderError,
                "The background terminal is not running.",
            )));
        }
        let events = self.shared.state.lock().background_stopped(&work.task_id);
        self.shared.emit_coalesced(events).await;
        Ok(())
    }

    async fn refresh_background_work(&self) -> DriverResult<()> {
        let listed = self.background_terminals().await?;
        let events = self
            .shared
            .state
            .lock()
            .background_listed(&self.shared, &listed);
        self.shared.emit_coalesced(events).await;
        Ok(())
    }

    fn has_pending_background_work(&self) -> bool {
        self.shared.state.lock().has_live_background()
    }

    fn computer_use(&self) -> Option<&dyn ComputerUseControl> {
        self.shared
            .computer_use
            .as_deref()
            .map(|control| control as &dyn ComputerUseControl)
    }

    async fn close(&self) -> DriverResult<()> {
        self.shared.conn.close().await;
        if let Some(computer_use) = &self.shared.computer_use {
            computer_use.cancel_computer_use();
        }
        Ok(())
    }
}

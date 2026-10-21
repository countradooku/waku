//! DeepSeek Harness as a `DriverV2` (RFC 0002 "DeepSeek Harness", #52).
//!
//! `dsh web` is the Harness client API: unary operations travel as typed
//! HTTP envelopes, while ordered session events, projections, approval
//! requests, questions and background jobs arrive on two WebSocket
//! downlinks (`/api/events.mux`, `/api/events.host`). One resident Host
//! serves every session that uses the same binary (`deepseek_pool`); a
//! session holds a pool lease and never stops a Host other sessions use.
//!
//! One actor task per session owns the downlinks and all stream state, and
//! handles one command at a time between events, so an event published
//! before a request's answer is processed after it.
//!
//! Agent presets are a provider-specific option fixed at `session.create`:
//! changing one starts a new provider thread with a handoff
//! (`CreateWithHandoff`).

mod normalize;
mod protocol;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt as _;
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use url::Url;
use waku_protocol::model::ProviderResumeCursor;
use waku_protocol::orchestration::{self as orch, *};

use self::normalize::{Effect, Interaction, Normalizer, PendingTurn};
use self::protocol::{HOST_ROUTE, MUX_ROUTE, Rpc};
use super::v2::{
    AdapterEvent, AdapterEventSender, Downlink, DriverError, DriverFamily, DriverResult,
    DriverSession, DriverV2, EnsureThread, EntityKind, ForkPoint, ForkRequest, FrameDirection,
    InstanceConfig, Interrupt, NativeRef, NotReadyReason, OpenedSession, ProviderThreadRef,
    ResumeThread, Rollback, RollbackOutcome, RuntimeReply, RuntimeResponse, Selection,
    SessionContext, Steer, SteerRejectReason, ThreadDisposition, ThreadSnapshot, TransitionPlan,
    TurnAccepted, TurnIntent, TurnOutcome, TurnStart, adapter_channel, wrap_synthetic_context,
};
use crate::deepseek_pool::PooledDeepSeekServer;

/// `ProviderKind::id()` of DeepSeek Harness, the replay fixture name.
pub const DEEPSEEK_PROVIDER: &str = "deepseek";
/// The replay protocol name (RFC 0002 "File format").
pub const DEEPSEEK_PROTOCOL: &str = "deepseek.dsh-web";

const DRIVER: orch::DriverKind = orch::DriverKind::DeepSeek;
const RESUBSCRIBE_MIN: Duration = Duration::from_millis(200);
const RESUBSCRIBE_MAX: Duration = Duration::from_secs(2);

/// The instance replay fixtures are recorded against: a fixed Host URL
/// instead of a pooled `dsh web`.
pub fn deepseek_replay_instance() -> InstanceConfig {
    let mut cfg = InstanceConfig::new(DEEPSEEK_PROVIDER, orch::DriverKind::DeepSeek);
    cfg.binary = "dsh".into();
    cfg.cli_version = Some("0.1.1".to_owned());
    cfg.extra = json!({ "baseUrl": "http://127.0.0.1:59258" });
    cfg
}

/// What the ported driver does (RFC 0002 matrix, DeepSeek column). Pure.
pub fn deepseek_capabilities(_cfg: &InstanceConfig) -> Capabilities {
    let mut caps = Capabilities::default();
    caps.sessions.model_switch_in_session = true;
    caps.sessions.provider_switching_via_handoff = true;
    caps.sessions.runtime_mode_switch_in_session = true;
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
    caps.streaming.emits_message_completed = true;
    caps.tools.tool_item_ids = true;
    caps.tools.emits_tool_started = true;
    caps.tools.emits_tool_completed = true;
    caps.tools.emits_tool_output = true;
    caps.approvals.command = true;
    caps.approvals.file_change = true;
    caps.approvals.native_request_ids = true;
    caps.approvals.callbacks_are_live_only = true;
    caps.planning.emits_plan_updated = true;
    caps.planning.emits_todo_list = true;
    caps.planning.structured_questions = true;
    caps.subagents.supported = true;
    caps.subagents.emits_lifecycle = true;
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
    caps.extras.background_work = true;
    caps.extras.agent_presets = true;
    caps.extras.model_discovery = true;
    caps
}

/// RFC 0002 "Selection transitions", DeepSeek column.
pub fn deepseek_transition(from: &Selection, to: &Selection) -> TransitionPlan {
    let mut plans = Vec::new();
    if from.model != to.model
        || from.reasoning_effort != to.reasoning_effort
        || from.context_window != to.context_window
        || from.runtime_mode != to.runtime_mode
    {
        // `session.selectModel`, and the permission command for the mode.
        plans.push(TransitionPlan::ApplyInSession);
    }
    if from.service_tier != to.service_tier {
        plans.push(TransitionPlan::ApplyOnNextTurn);
    }
    if from.agent_preset != to.agent_preset {
        // The preset is fixed at `session.create`.
        plans.push(TransitionPlan::CreateWithHandoff);
    }
    if from.interaction_mode != to.interaction_mode {
        plans.push(TransitionPlan::Reject {
            reason: "DeepSeek Harness has no plan mode Waku can drive".to_owned(),
        });
    }
    TransitionPlan::combine(plans)
}

/// Harness's permission level for a runtime mode.
fn permission_for(mode: RuntimeMode) -> &'static str {
    if mode == RuntimeMode::FullAccess {
        "danger-full-access"
    } else {
        "workspace-write"
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DeepSeekDriver;

#[async_trait]
impl DriverV2 for DeepSeekDriver {
    fn kind(&self) -> DriverFamily {
        DriverFamily::DeepSeek
    }

    fn capabilities(&self, cfg: &InstanceConfig) -> Capabilities {
        deepseek_capabilities(cfg)
    }

    fn plan_selection_transition(
        &self,
        _session_caps: &Capabilities,
        from: &Selection,
        to: &Selection,
    ) -> TransitionPlan {
        deepseek_transition(from, to)
    }

    async fn open_session(&self, ctx: SessionContext) -> DriverResult<OpenedSession> {
        let capabilities = self.capabilities(&ctx.instance);
        let (base, lease) = host_url(&ctx.instance).await?;
        let http = ctx.transports.http("main", base).await?;
        // Subscribe first: a create publishes host and mux state at once.
        let mux = http.websocket(MUX_ROUTE).await?;
        let host = http.websocket(HOST_ROUTE).await?;
        let (events, receiver) = adapter_channel();
        let (commands, inbox) = mpsc::unbounded_channel();
        let jobs = Arc::new(AtomicUsize::new(0));
        let requested = ctx.cwd.clone();
        let cwd = tokio::task::spawn_blocking(move || {
            std::fs::canonicalize(&requested).unwrap_or(requested)
        })
        .await
        .unwrap_or_else(|_| ctx.cwd.clone());
        let actor = SessionActor {
            rpc: Rpc::new(http),
            lease,
            cwd: cwd.to_string_lossy().into_owned(),
            ids: ctx.ids.clone(),
            raw_log: ctx.raw_log.clone(),
            provider_session_id: ctx.provider_session_id,
            thread_id: ctx.thread_id,
            instance_id: ctx.instance.instance_id.clone(),
            clock: ctx.clock.clone(),
            norm: Normalizer::new(
                ctx.ids.clone(),
                ctx.clock.clone(),
                ctx.provider_session_id,
                ctx.instance.instance_id.clone(),
                ctx.selection.runtime_mode,
            ),
            events: Some(events),
            native: None,
            agent_preset: None,
            permission: None,
            model: None,
            commands_cache: None,
            jobs: jobs.clone(),
        };
        tokio::spawn(actor.run(inbox, ctx.shutdown.clone(), mux, host));
        Ok(OpenedSession {
            session: Arc::new(DeepSeekSession {
                commands,
                capabilities: capabilities.clone(),
                jobs,
            }),
            events: receiver,
            capabilities,
        })
    }
}

/// The Host's URL, and the pool lease that keeps a pooled Host alive. An
/// instance may name a Host in `extra.baseUrl` (replay, a Host the user runs
/// themselves); otherwise the pooled `dsh web` for the binary is used.
async fn host_url(instance: &InstanceConfig) -> DriverResult<(Url, Option<PooledDeepSeekServer>)> {
    if let Some(url) = instance.extra.get("baseUrl").and_then(Value::as_str) {
        let url = Url::parse(url).map_err(|error| DriverError::Protocol {
            reason: super::v2::ProtocolReason::Decode,
            detail: format!("invalid baseUrl: {error}"),
        })?;
        return Ok((url, None));
    }
    let binary = instance.binary.clone();
    let lease = tokio::task::spawn_blocking(move || crate::deepseek_pool::acquire(&binary))
        .await
        .map_err(|error| DriverError::SessionLost {
            detail: error.to_string(),
        })?
        .map_err(|error| {
            DriverError::Provider(
                ProviderFailure::new(FailureClass::TransportError, error.to_string())
                    .with_code("host_unavailable"),
            )
        })?;
    let url = Url::parse(&format!("http://127.0.0.1:{}", lease.port)).map_err(|error| {
        DriverError::Protocol {
            reason: super::v2::ProtocolReason::Decode,
            detail: error.to_string(),
        }
    })?;
    Ok((url, Some(lease)))
}

type Reply<T> = oneshot::Sender<DriverResult<T>>;

enum Command {
    Ensure(EnsureThread, Reply<ProviderThreadRef>),
    Resume(ResumeThread, Reply<ProviderThreadRef>),
    Start(Box<TurnStart>, Reply<TurnAccepted>),
    Steer(Steer, Reply<()>),
    Interrupt(Interrupt, Reply<()>),
    Respond(RuntimeResponse, Reply<()>),
    Snapshot(ProviderThreadRef, Reply<ThreadSnapshot>),
    Rollback(Rollback, Reply<RollbackOutcome>),
    Fork(ForkRequest, Reply<ProviderThreadRef>),
    Apply(Selection, Reply<()>),
    Close(oneshot::Sender<()>),
}

struct DeepSeekSession {
    commands: mpsc::UnboundedSender<Command>,
    capabilities: Capabilities,
    jobs: Arc<AtomicUsize>,
}

impl DeepSeekSession {
    async fn call<T>(&self, command: impl FnOnce(Reply<T>) -> Command) -> DriverResult<T> {
        let lost = || DriverError::SessionLost {
            detail: "the DeepSeek Harness session closed".to_owned(),
        };
        let (reply, answer) = oneshot::channel();
        self.commands.send(command(reply)).map_err(|_| lost())?;
        answer.await.map_err(|_| lost())?
    }
}

#[async_trait]
impl DriverSession for DeepSeekSession {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn ensure_thread(&self, req: EnsureThread) -> DriverResult<ProviderThreadRef> {
        self.call(|reply| Command::Ensure(req, reply)).await
    }

    async fn resume_thread(&self, req: ResumeThread) -> DriverResult<ProviderThreadRef> {
        self.call(|reply| Command::Resume(req, reply)).await
    }

    async fn start_turn(&self, req: TurnStart) -> DriverResult<TurnAccepted> {
        self.call(|reply| Command::Start(Box::new(req), reply))
            .await
    }

    async fn steer_turn(&self, req: Steer) -> DriverResult<()> {
        self.call(|reply| Command::Steer(req, reply)).await
    }

    async fn interrupt_turn(&self, req: Interrupt) -> DriverResult<()> {
        self.call(|reply| Command::Interrupt(req, reply)).await
    }

    async fn respond(&self, req: RuntimeResponse) -> DriverResult<()> {
        self.call(|reply| Command::Respond(req, reply)).await
    }

    async fn read_snapshot(&self, thread: &ProviderThreadRef) -> DriverResult<ThreadSnapshot> {
        let thread = thread.clone();
        self.call(|reply| Command::Snapshot(thread, reply)).await
    }

    async fn rollback(&self, req: Rollback) -> DriverResult<RollbackOutcome> {
        self.call(|reply| Command::Rollback(req, reply)).await
    }

    async fn fork(&self, req: ForkRequest) -> DriverResult<ProviderThreadRef> {
        self.call(|reply| Command::Fork(req, reply)).await
    }

    async fn apply_selection(&self, to: &Selection) -> DriverResult<()> {
        let to = to.clone();
        self.call(|reply| Command::Apply(to, reply)).await
    }

    fn has_pending_background_work(&self) -> bool {
        self.jobs.load(Ordering::Acquire) > 0
    }

    async fn close(&self) -> DriverResult<()> {
        let (reply, done) = oneshot::channel();
        if self.commands.send(Command::Close(reply)).is_ok() {
            let _ = done.await;
        }
        Ok(())
    }
}

struct SessionActor {
    rpc: Rpc,
    /// Keeps a pooled Host alive; released (off the runtime) on close.
    lease: Option<PooledDeepSeekServer>,
    cwd: String,
    ids: Arc<dyn super::v2::IdAllocator>,
    raw_log: Arc<dyn super::v2::RawFrameLog>,
    provider_session_id: ProviderSessionId,
    thread_id: ThreadId,
    instance_id: ProviderInstanceId,
    clock: Arc<dyn super::v2::Clock>,
    norm: Normalizer,
    events: Option<AdapterEventSender>,
    /// The native session this actor drives (changes on a rollback).
    native: Option<String>,
    agent_preset: Option<String>,
    /// The permission level last applied.
    permission: Option<String>,
    model: Option<(Option<String>, Option<String>)>,
    commands_cache: Option<HashSet<String>>,
    jobs: Arc<AtomicUsize>,
}

enum Next {
    Command(Option<Command>),
    Envelope(&'static str, Option<std::io::Result<Value>>),
    Tick,
    Resubscribe,
    Shutdown,
}

async fn next_of(downlink: &mut Option<Downlink>) -> Option<std::io::Result<Value>> {
    match downlink {
        Some(downlink) => downlink.next().await,
        None => std::future::pending().await,
    }
}

async fn sleep_until(at: Option<tokio::time::Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

impl SessionActor {
    async fn emit(&self, events: Vec<AdapterEvent>) {
        let Some(sender) = &self.events else { return };
        for event in events {
            if sender.send(event).await.is_err() {
                return;
            }
        }
    }

    fn note(&self, text: &str) {
        self.raw_log.record("main", FrameDirection::Note, text);
    }

    /// Whether the Host behind this session is still running. A Host given
    /// by URL has no process to ask: a closed downlink means it is gone.
    fn host_alive(&self) -> bool {
        self.lease.as_ref().is_some_and(|lease| lease.is_alive())
    }

    async fn run(
        mut self,
        mut inbox: mpsc::UnboundedReceiver<Command>,
        shutdown: super::v2::Shutdown,
        mux: Downlink,
        host: Downlink,
    ) {
        let mut mux = Some(mux);
        let mut host = Some(host);
        let mut backoff = RESUBSCRIBE_MIN;
        let mut resubscribe_at: Option<tokio::time::Instant> = None;
        loop {
            let tick = self.norm.has_pending_snapshots();
            let next = tokio::select! {
                biased;
                command = inbox.recv() => Next::Command(command),
                envelope = next_of(&mut mux) => Next::Envelope(MUX_ROUTE, envelope),
                envelope = next_of(&mut host) => Next::Envelope(HOST_ROUTE, envelope),
                () = sleep_until(resubscribe_at), if resubscribe_at.is_some() => Next::Resubscribe,
                () = tokio::time::sleep(Duration::from_millis(16)), if tick => Next::Tick,
                () = shutdown.cancelled() => Next::Shutdown,
            };
            match next {
                Next::Command(None) | Next::Shutdown => break,
                Next::Command(Some(command)) => {
                    if !self.handle(command).await {
                        break;
                    }
                }
                Next::Envelope(_, Some(Ok(envelope))) => {
                    backoff = RESUBSCRIBE_MIN;
                    self.on_envelope(envelope).await;
                }
                Next::Envelope(route, ended) => {
                    if let Some(Err(error)) = &ended {
                        self.note(&format!("{route} ended: {error}"));
                    }
                    if route == MUX_ROUTE {
                        mux = None;
                    } else {
                        host = None;
                    }
                    if !self.host_alive() {
                        self.host_lost().await;
                        break;
                    }
                    resubscribe_at = Some(tokio::time::Instant::now() + backoff);
                    backoff = (backoff * 2).min(RESUBSCRIBE_MAX);
                }
                Next::Resubscribe => {
                    resubscribe_at = None;
                    if mux.is_none() {
                        match self.rpc.transport().websocket(MUX_ROUTE).await {
                            Ok(downlink) => mux = Some(downlink),
                            Err(error) => self.note(&format!("mux resubscribe failed: {error}")),
                        }
                    }
                    if host.is_none() {
                        match self.rpc.transport().websocket(HOST_ROUTE).await {
                            Ok(downlink) => host = Some(downlink),
                            Err(error) => self.note(&format!("host resubscribe failed: {error}")),
                        }
                    }
                    if mux.is_none() || host.is_none() {
                        if !self.host_alive() {
                            self.host_lost().await;
                            break;
                        }
                        resubscribe_at = Some(tokio::time::Instant::now() + backoff);
                        backoff = (backoff * 2).min(RESUBSCRIBE_MAX);
                    } else {
                        // `session/subscribed` reports anything missed.
                        self.catch_up().await;
                    }
                }
                Next::Tick => {
                    let due = self.norm.due();
                    self.emit(due).await;
                }
            }
        }
        drop((mux, host));
        self.events = None;
        // The last lease of a pooled Host stops it, which blocks.
        if let Some(lease) = self.lease.take() {
            let _ = tokio::task::spawn_blocking(move || drop(lease)).await;
        }
    }

    /// The Host exited: the open turn fails and the session's event stream
    /// ends, so the manager reopens it (`SessionLost`).
    async fn host_lost(&mut self) {
        let failure =
            ProviderFailure::new(FailureClass::TransportError, "DeepSeek Harness exited.")
                .with_code("process_exit");
        let events = self
            .norm
            .finish_turn(TurnOutcome::Failed { failure }, ThreadDisposition::Reusable);
        self.emit(events).await;
    }

    async fn on_envelope(&mut self, envelope: Value) {
        let payload = envelope.get("payload").unwrap_or(&envelope);
        // Session frames go to their session; session-less ones (host
        // state, command changes) to every session.
        if let Some(session) = payload.get("sessionId").and_then(Value::as_str)
            && self.native.as_deref() != Some(session)
        {
            return;
        }
        if self.native.is_none() {
            return;
        }
        let (events, effects) = self.norm.on_envelope(&envelope);
        if events.is_empty() && effects.is_empty() {
            self.raw_log
                .record("main", FrameDirection::Inbound, &envelope.to_string());
        }
        self.jobs.store(self.norm.jobs.len(), Ordering::Release);
        self.emit(events).await;
        for effect in effects {
            self.effect(effect).await;
        }
    }

    async fn effect(&mut self, effect: Effect) {
        let Some(session) = self.native.clone() else {
            return;
        };
        match effect {
            Effect::AutoApprove {
                rpc_id,
                approval_id,
            } => {
                let value = json!({
                    "sessionId": session,
                    "approvalId": approval_id,
                    "outcome": normalize::OUTCOME_ALLOW_ONCE,
                });
                if let Err(error) = self
                    .rpc
                    .respond(&rpc_id, json!({"ok": true, "value": value}))
                    .await
                {
                    self.note(&format!("automatic approval failed: {error:?}"));
                }
            }
            Effect::RejectQuestion { rpc_id } => {
                let _ = self
                    .rpc
                    .respond(
                        &rpc_id,
                        cancelled("the question request contained no questions"),
                    )
                    .await;
            }
            Effect::CatchUp => self.catch_up().await,
            Effect::CommandsChanged => self.commands_cache = None,
        }
    }

    /// Replays stored events past the newest one seen.
    async fn catch_up(&mut self) {
        let Some(session) = self.native.clone() else {
            return;
        };
        match self.rpc.history(&session).await {
            Ok(history) => {
                let mut events = Vec::new();
                for entry in &history.entries {
                    let event = entry.get("event").unwrap_or(entry);
                    events.extend(self.norm.session_event(event, entry.get("view")));
                }
                self.emit(events).await;
            }
            Err(error) => self.note(&format!("history catch-up failed: {error:?}")),
        }
    }

    fn thread_ref(&self, provider_thread_id: ProviderThreadId, native: &str) -> ProviderThreadRef {
        ProviderThreadRef {
            provider_thread_id,
            native: NativeRef::strong(native),
            cursor: Some(ProviderResumeCursor::DeepSeek {
                session_id: native.to_owned(),
            }),
        }
    }

    fn follow(&mut self, native: String, provider_thread_id: ProviderThreadId) {
        self.native = Some(native);
        self.norm.provider_thread = Some(provider_thread_id);
    }

    async fn handle(&mut self, command: Command) -> bool {
        match command {
            Command::Ensure(req, reply) => {
                let _ = reply.send(self.ensure(req).await);
            }
            Command::Resume(req, reply) => {
                let _ = reply.send(self.resume(req).await);
            }
            Command::Start(req, reply) => {
                let _ = reply.send(self.start(*req).await);
            }
            Command::Steer(req, reply) => {
                let _ = reply.send(self.steer(req).await);
            }
            Command::Interrupt(req, reply) => {
                let _ = reply.send(self.interrupt(req).await);
            }
            Command::Respond(req, reply) => {
                let _ = reply.send(self.respond(req).await);
            }
            Command::Snapshot(thread, reply) => {
                let _ = reply.send(self.snapshot(&thread).await);
            }
            Command::Rollback(req, reply) => {
                let _ = reply.send(self.rollback(req).await);
            }
            Command::Fork(req, reply) => {
                let _ = reply.send(self.fork(req).await);
            }
            Command::Apply(selection, reply) => {
                let _ = reply.send(self.apply(&selection).await);
            }
            Command::Close(reply) => {
                let _ = reply.send(());
                return false;
            }
        }
        true
    }

    /// `session.create` with a client-minted id; the Host's answer names the
    /// session (it is adopted if it differs).
    async fn create(&self, preset: Option<&str>, session_id: &str) -> DriverResult<String> {
        let created = self
            .rpc
            .create_session(&self.cwd, session_id, preset)
            .await
            .map_err(|error| error.into_driver("open a session"))?;
        Ok(created
            .get("sessionId")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .unwrap_or(session_id)
            .to_owned())
    }

    /// Applies the session options a fresh or attached session needs:
    /// clears a legacy read-only plan state, and sets the permission level.
    async fn configure(
        &mut self,
        session: &str,
        selection: &Selection,
        resumed: bool,
    ) -> DriverResult<()> {
        let history = self
            .rpc
            .history(session)
            .await
            .map_err(|error| error.into_driver("read the session history"))?;
        let (turns, last_seq) = history.turns();
        self.norm.follow(last_seq, turns);
        if resumed && history.legacy_read_only() {
            self.execute(session, "/plan off").await?;
        }
        self.permission = history.permission().map(str::to_owned);
        self.sync_selection(session, selection).await
    }

    async fn execute(&self, session: &str, line: &str) -> DriverResult<Option<String>> {
        match self.rpc.execute(session, line).await {
            Ok((true, text)) => Ok(text),
            Ok((false, text)) => Err(DriverError::Provider(ProviderFailure::new(
                FailureClass::ProviderError,
                text.unwrap_or_else(|| format!("DeepSeek Harness could not run {line}")),
            ))),
            Err(error) => Err(error.into_driver("run a command")),
        }
    }

    async fn sync_selection(&mut self, session: &str, selection: &Selection) -> DriverResult<()> {
        let model = (selection.model.clone(), selection.reasoning_effort.clone());
        if let Some(name) = &selection.model
            && self.model.as_ref() != Some(&model)
        {
            self.rpc
                .select_model(session, name, selection.reasoning_effort.as_deref())
                .await
                .map_err(|error| error.into_driver("select the model"))?;
        }
        self.model = Some(model);
        let permission = permission_for(selection.runtime_mode);
        if self.permission.as_deref() != Some(permission) {
            self.execute(session, &format!("/permission {permission}"))
                .await?;
            self.permission = Some(permission.to_owned());
        }
        self.norm.mode = selection.runtime_mode;
        Ok(())
    }

    async fn ensure(&mut self, req: EnsureThread) -> DriverResult<ProviderThreadRef> {
        let preset = req
            .selection
            .agent_preset
            .clone()
            .filter(|preset| !preset.is_empty());
        let minted = uuid::Uuid::new_v4().to_string();
        let native = self.create(preset.as_deref(), &minted).await?;
        self.agent_preset = preset;
        let provider_thread_id = match &req.existing {
            Some(existing) => existing.provider_thread_id,
            None => ProviderThreadId(self.ids.bind(
                self.provider_session_id.0,
                EntityKind::ProviderThread,
                &native,
            )),
        };
        self.follow(native.clone(), provider_thread_id);
        self.configure(&native, &req.selection, false).await?;
        Ok(self.thread_ref(provider_thread_id, &native))
    }

    async fn resume(&mut self, req: ResumeThread) -> DriverResult<ProviderThreadRef> {
        let requested = match &req.thread.cursor {
            Some(ProviderResumeCursor::DeepSeek { session_id }) if !session_id.is_empty() => {
                session_id.clone()
            }
            _ => req.thread.native.id.clone(),
        };
        // Attaching is `session.create` with the existing id.
        let native = self.create(None, &requested).await?;
        self.follow(native.clone(), req.thread.provider_thread_id);
        self.configure(&native, &req.selection, true).await?;
        Ok(self.thread_ref(req.thread.provider_thread_id, &native))
    }

    fn bound_session(&mut self, thread: &ProviderThreadRef) -> DriverResult<String> {
        if thread.is_bound() && self.native.as_deref() != Some(thread.native.id.as_str()) {
            self.follow(thread.native.id.clone(), thread.provider_thread_id);
        }
        self.native
            .clone()
            .ok_or(DriverError::NotReady(NotReadyReason::ThreadMissing))
    }

    async fn is_command(&mut self, session: &str, name: &str) -> bool {
        if self.commands_cache.is_none() {
            let names = self.rpc.command_names(session).await.unwrap_or_default();
            self.commands_cache = Some(names.into_iter().collect());
        }
        self.commands_cache
            .as_ref()
            .is_some_and(|names| names.contains(name))
    }

    async fn start(&mut self, req: TurnStart) -> DriverResult<TurnAccepted> {
        if self.norm.turn.is_some() || self.norm.pending_turn.is_some() {
            return Err(DriverError::NotReady(NotReadyReason::PreviousTurnActive));
        }
        let session = self.bound_session(&req.thread)?;
        self.sync_selection(&session, &req.selection).await?;
        let command = match &req.intent {
            TurnIntent::SlashCommand { name } => Some(name.clone()),
            _ => req
                .message
                .text
                .strip_prefix('/')
                .and_then(|line| line.split_whitespace().next())
                .filter(|name| !name.is_empty())
                .map(str::to_owned),
        };
        let pending = PendingTurn {
            run: req.run,
            ordinal: req.provider_turn_ordinal,
            provider_thread_id: req.thread.provider_thread_id,
        };
        if let Some(name) = command
            && self.is_command(&session, &name).await
        {
            return self.run_command(&session, &req, pending).await;
        }
        let text = wrap_synthetic_context(&req.app_context, &req.message.text);
        self.rpc
            .prompt(&session, &text, "queue")
            .await
            .map_err(|error| error.into_driver("start the turn"))?;
        // The turn opens at `turn/start`, whose seq is its native id.
        self.norm.pending_turn = Some(pending);
        Ok(TurnAccepted::default())
    }

    /// A Harness command line runs to completion in its RPC: the turn opens
    /// and ends around it.
    async fn run_command(
        &mut self,
        session: &str,
        req: &TurnStart,
        pending: PendingTurn,
    ) -> DriverResult<TurnAccepted> {
        let result = self.rpc.execute(session, &req.message.text).await;
        let reference = super::v2::ProviderTurnRef {
            provider_turn_id: ProviderTurnId(self.ids.bind_ordinal(
                pending.provider_thread_id.0,
                EntityKind::ProviderTurn,
                u64::from(pending.ordinal),
            )),
            native: NativeRef {
                id: String::new(),
                strength: RefStrength::None,
            },
            ordinal: pending.ordinal,
        };
        self.norm.turn = Some(normalize::OpenTurn {
            run: pending.run,
            reference: reference.clone(),
            provider_thread_id: pending.provider_thread_id,
            started_at: self.clock.now(),
            interrupted: false,
            usage: None,
        });
        let turn = self.norm.turn.as_ref().expect("just set");
        let mut events = vec![AdapterEvent::ProviderTurnUpdated {
            turn: self.norm.turn_snapshot(turn, ProviderTurnStatus::Running),
        }];
        let outcome = match result {
            Ok((true, _)) => TurnOutcome::Completed,
            Ok((false, text)) => TurnOutcome::Failed {
                failure: ProviderFailure::new(
                    FailureClass::ProviderError,
                    text.unwrap_or_else(|| "DeepSeek Harness command failed".to_owned()),
                ),
            },
            Err(error) => TurnOutcome::Failed {
                failure: error.into_driver("run the command").to_failure(),
            },
        };
        events.extend(self.norm.finish_turn(outcome, ThreadDisposition::Reusable));
        self.emit(events).await;
        Ok(TurnAccepted {
            provider_turn: Some(reference),
        })
    }

    async fn steer(&mut self, req: Steer) -> DriverResult<()> {
        let active = self
            .norm
            .turn
            .as_ref()
            .map(|turn| turn.run.attempt_id)
            .or(self
                .norm
                .pending_turn
                .as_ref()
                .map(|pending| pending.run.attempt_id));
        match active {
            None => {
                return Err(DriverError::SteerRejected {
                    reason: SteerRejectReason::NoActiveTurn,
                });
            }
            Some(attempt) if attempt != req.run.attempt_id => {
                return Err(DriverError::SteerRejected {
                    reason: SteerRejectReason::TurnSettled,
                });
            }
            Some(_) => {}
        }
        let session = self.bound_session(&req.thread)?;
        self.rpc
            .prompt(&session, &req.message.text, "steer")
            .await
            .map(drop)
            .map_err(|_| DriverError::SteerRejected {
                reason: SteerRejectReason::ProviderRefused,
            })
    }

    async fn interrupt(&mut self, req: Interrupt) -> DriverResult<()> {
        let Some(session) = self.native.clone() else {
            return Ok(());
        };
        if let Some(turn) = self.norm.turn.as_mut() {
            if turn.run.attempt_id != req.run.attempt_id {
                return Ok(());
            }
            turn.interrupted = true;
            // Its `turn/end` (`aborted`) is the terminal.
            return self
                .rpc
                .cancel(&session)
                .await
                .map_err(|error| error.into_driver("stop the turn"));
        }
        if self
            .norm
            .pending_turn
            .as_ref()
            .is_some_and(|pending| pending.run.attempt_id == req.run.attempt_id)
        {
            // Accepted but not started: cancel it, end it now, and drop the
            // events of the turn the Host may still open for it.
            let _ = self.rpc.cancel(&session).await;
            let events = self
                .norm
                .finish_turn(TurnOutcome::Cancelled, ThreadDisposition::Reusable);
            self.norm.swallow = true;
            self.emit(events).await;
        }
        Ok(())
    }

    async fn respond(&mut self, req: RuntimeResponse) -> DriverResult<()> {
        let Some(session) = self.native.clone() else {
            return Err(DriverError::NotReady(NotReadyReason::ThreadMissing));
        };
        let rpc_id = req.native.id.clone();
        let Some(pending) = self.norm.requests.get(&rpc_id) else {
            return Ok(());
        };
        let result = match (&pending.interaction, req.reply) {
            (Interaction::Approval { approval_id }, RuntimeReply::Decision { option_id }) => {
                json!({
                    "ok": true,
                    "value": {"sessionId": session, "approvalId": approval_id, "outcome": option_id},
                })
            }
            (Interaction::Approval { approval_id }, RuntimeReply::Dismiss) => json!({
                "ok": true,
                "value": {
                    "sessionId": session,
                    "approvalId": approval_id,
                    "outcome": normalize::OUTCOME_REJECT,
                },
            }),
            (Interaction::Question { questions }, RuntimeReply::Answers(answers)) => json!({
                "ok": true,
                "value": {
                    "sessionId": session,
                    "answer": {"answers": normalize::question_answers(questions, &answers)},
                },
            }),
            (Interaction::Question { .. }, RuntimeReply::Dismiss) => {
                cancelled("the user dismissed the question")
            }
            _ => {
                return Err(DriverError::Unsupported {
                    operation: "respond",
                });
            }
        };
        self.rpc
            .respond(&rpc_id, result)
            .await
            .map_err(|error| error.into_driver("answer the request"))?;
        let events = self.norm.resolve(&rpc_id, RequestStatus::Resolved);
        self.emit(events).await;
        Ok(())
    }

    /// The `turn/end` seq to fork at for the first `kept` turns. A turn is
    /// found by its native (`turn/start`) seq, else by count.
    fn fork_seq(&self, kept: &[super::v2::ProviderTurnRef]) -> Option<u64> {
        let last = kept.last()?;
        last.native
            .id
            .parse::<u64>()
            .ok()
            .and_then(|start| {
                self.norm
                    .completed
                    .iter()
                    .find(|(turn_start, _)| *turn_start == start)
                    .map(|(_, end)| *end)
            })
            .or_else(|| self.norm.completed.get(kept.len() - 1).map(|(_, end)| *end))
    }

    /// Replaced: `session.fork { atSeq }` at the end of the last kept turn,
    /// or a fresh `session.create` when no turn is kept.
    async fn rollback(&mut self, req: Rollback) -> DriverResult<RollbackOutcome> {
        if req.turns_to_remove == 0 {
            return Ok(RollbackOutcome {
                thread: req.thread,
                replaced: false,
                snapshot: None,
                retained_native_turn_ids: None,
            });
        }
        let session = self.bound_session(&req.thread)?;
        let kept = req.turns.len().saturating_sub(req.turns_to_remove as usize);
        let (native, completed, last_seq) = if kept == 0 {
            let minted = uuid::Uuid::new_v4().to_string();
            let preset = self.agent_preset.clone();
            (
                self.create(preset.as_deref(), &minted).await?,
                Vec::new(),
                -1,
            )
        } else {
            let at = self
                .fork_seq(&req.turns[..kept])
                .ok_or(DriverError::Protocol {
                    reason: super::v2::ProtocolReason::UnexpectedFrame,
                    detail: "no completed turn to fork at".to_owned(),
                })?;
            let forked = self
                .rpc
                .fork(&session, at)
                .await
                .map_err(|error| error.into_driver("fork the session"))?;
            let completed = self
                .norm
                .completed
                .iter()
                .copied()
                .filter(|(_, end)| *end <= at)
                .collect();
            (forked, completed, i64::try_from(at).unwrap_or(i64::MAX))
        };
        let provider_thread_id = ProviderThreadId(self.ids.bind(
            req.thread.provider_thread_id.0,
            EntityKind::ProviderThread,
            &native,
        ));
        self.follow(native.clone(), provider_thread_id);
        self.norm.follow(last_seq, completed);
        Ok(RollbackOutcome {
            thread: self.thread_ref(provider_thread_id, &native),
            replaced: true,
            snapshot: None,
            retained_native_turn_ids: Some(
                req.turns[..kept]
                    .iter()
                    .map(|turn| turn.native.id.clone())
                    .collect(),
            ),
        })
    }

    async fn fork(&mut self, req: ForkRequest) -> DriverResult<ProviderThreadRef> {
        let session = req.source.native.id.clone();
        let native = match &req.at {
            ForkPoint::SubagentThread(_) => {
                return Err(DriverError::Unsupported {
                    operation: "fork_from_subagent_thread",
                });
            }
            ForkPoint::Tip => {
                let at = self.norm.completed.last().map(|(_, end)| *end).ok_or(
                    DriverError::Protocol {
                        reason: super::v2::ProtocolReason::UnexpectedFrame,
                        detail: "no completed turn to fork at".to_owned(),
                    },
                )?;
                self.rpc.fork(&session, at).await
            }
            ForkPoint::AfterProviderTurn { turn, .. } => {
                let at =
                    self.fork_seq(std::slice::from_ref(turn))
                        .ok_or(DriverError::Protocol {
                            reason: super::v2::ProtocolReason::UnexpectedFrame,
                            detail: "the fork turn has not completed".to_owned(),
                        })?;
                self.rpc.fork(&session, at).await
            }
        }
        .map_err(|error| error.into_driver("fork the session"))?;
        Ok(self.thread_ref(
            ProviderThreadId(self.ids.bind(
                req.source.provider_thread_id.0,
                EntityKind::ProviderThread,
                &native,
            )),
            &native,
        ))
    }

    async fn apply(&mut self, selection: &Selection) -> DriverResult<()> {
        match self.native.clone() {
            Some(session) => self.sync_selection(&session, selection).await,
            None => {
                self.norm.mode = selection.runtime_mode;
                Ok(())
            }
        }
    }

    /// The stored conversation: user and assistant messages, and one turn row
    /// per completed native turn.
    async fn snapshot(&self, thread: &ProviderThreadRef) -> DriverResult<ThreadSnapshot> {
        let history = self
            .rpc
            .history(&thread.native.id)
            .await
            .map_err(|error| error.into_driver("read the session history"))?;
        let now = self.clock.now();
        let pt = thread.provider_thread_id;
        let (turns, _) = history.turns();
        let turns = turns
            .iter()
            .enumerate()
            .map(|(index, (start, _))| {
                let native = start.to_string();
                ProviderTurn {
                    id: ProviderTurnId(self.ids.bind(pt.0, EntityKind::ProviderTurn, &native)),
                    provider_thread_id: pt,
                    node_id: NodeId(self.ids.bind(pt.0, EntityKind::Node, &native)),
                    run_attempt_id: None,
                    native_turn_ref: Some(NativeRef::strong(native).to_provider_ref(DRIVER)),
                    ordinal: index as u32 + 1,
                    status: ProviderTurnStatus::Completed,
                    started_at: None,
                    completed_at: None,
                    token_usage: None,
                    turn_token_usage: None,
                }
            })
            .collect();
        let mut messages = Vec::new();
        for entry in &history.entries {
            let event = entry.get("event").unwrap_or(entry);
            let (role, text) = match event.get("type").and_then(Value::as_str) {
                Some("user/message") => (
                    MessageRole::User,
                    event.pointer("/data/text").and_then(Value::as_str),
                ),
                Some("assistant/message") => (
                    MessageRole::Assistant,
                    event
                        .pointer("/data/message/content/0/text")
                        .and_then(Value::as_str),
                ),
                _ => continue,
            };
            let Some(text) = text else { continue };
            let seq = event.get("seq").and_then(Value::as_u64).unwrap_or_default();
            messages.push(Message {
                id: MessageId(
                    self.ids
                        .bind(pt.0, EntityKind::Message, &format!("seq:{seq}")),
                ),
                thread_id: self.thread_id,
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
        let mut row = ProviderThread {
            id: pt,
            driver: DRIVER,
            provider_instance_id: self.instance_id.clone(),
            provider_session_id: Some(self.provider_session_id),
            app_thread_id: Some(self.thread_id),
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
        Ok(ThreadSnapshot {
            thread: row,
            turns,
            messages,
            pending_requests: self
                .norm
                .requests
                .values()
                .map(|pending| pending.request.clone())
                .collect(),
        })
    }
}

/// A Host request answered with a cancellation.
fn cancelled(message: &str) -> Value {
    json!({
        "ok": false,
        "error": {"code": "cancelled", "message": message, "details": {}},
    })
}

#[cfg(test)]
mod tests;

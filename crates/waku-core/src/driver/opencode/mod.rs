//! OpenCode as a `DriverV2` (RFC 0002 "OpenCode", #49).
//!
//! Waku does not own an OpenCode process. `opencode_service` finds the
//! background service the user's own terminal started (or starts one with
//! `opencode serve --service`), and every session talks to it over HTTP
//! through `ctx.transports.http`: requests for every operation, plus one
//! `GET /api/event` downlink per session, filtered by the native session id.
//!
//! One actor task per session owns the downlink, the request client and all
//! stream state. It handles one command at a time and reads events between
//! commands, so an event the service publishes before a request's answer is
//! processed after that answer: a session created by the service (no
//! client-minted id) cannot race its first events.
//!
//! Rules carried over from the v1 driver:
//! * **The session never owns the service.** `close` unsubscribes; nothing
//!   signals the process or calls a stop route.
//! * **A stream break is not `SessionLost`.** The actor resubscribes, then
//!   reconciles: re-lists pending permissions and forms, and settles a turn
//!   whose execution ended while the stream was down.
//! * **`always` never goes on the wire.** It would write the GLOBAL
//!   `/api/permission/saved`; durable choices stay in driver state
//!   (`driver::support`) and every reply is one-shot.
//! * **Execution outcomes are the terminal.** `session.idle` is never waited
//!   for.

mod normalize;
mod protocol;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt as _;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use url::Url;
use waku_protocol::model::ProviderResumeCursor;
use waku_protocol::orchestration::{self as orch, *};

use self::normalize::{Effect, Normalizer, OpenTurn};
use self::protocol::{Api, ApiError, Delivery, EVENT_ROUTE};
use super::v2::{
    AdapterEvent, AdapterEventSender, ContextBlock, Downlink, DriverError, DriverFamily,
    DriverResult, DriverSession, DriverV2, EnsureThread, EntityKind, ForkPoint, ForkRequest,
    FrameDirection, InstanceConfig, Interrupt, NativeRef, NotReadyReason, OpenedSession,
    ProviderThreadRef, ProviderTurnRef, ResumeThread, Rollback, RollbackOutcome, RuntimeReply,
    RuntimeResponse, Selection, SessionContext, Steer, SteerRejectReason, ThreadDisposition,
    ThreadSnapshot, TransitionPlan, TurnAccepted, TurnIntent, TurnOutcome, TurnStart,
    adapter_channel, wrap_synthetic_context,
};
use crate::opencode_api::{
    FormAnswer, FormField, FormValue, MessageInfo, ModelRef, PermissionReply,
};

/// `ProviderKind::id()` of OpenCode, the replay fixture name.
pub const OPENCODE_PROVIDER: &str = "opencode";
/// The replay protocol name (RFC 0002 "File format").
pub const OPENCODE_PROTOCOL: &str = "opencode.http-sse";

const DRIVER: orch::DriverKind = orch::DriverKind::OpenCode;
const RESUBSCRIBE_MIN: Duration = Duration::from_millis(250);
const RESUBSCRIBE_MAX: Duration = Duration::from_secs(2);

/// The instance replay fixtures are recorded against: a fixed service URL
/// instead of discovering (or starting) the user's service.
pub fn opencode_replay_instance() -> InstanceConfig {
    let mut cfg = InstanceConfig::new(OPENCODE_PROVIDER, orch::DriverKind::OpenCode);
    cfg.binary = "opencode".into();
    cfg.cli_version = Some("2.0.22".to_owned());
    cfg.extra = serde_json::json!({ "serviceUrl": "http://opencode:replay@127.0.0.1:4096" });
    cfg
}

/// What the ported driver does (RFC 0002 matrix, OpenCode column). Pure.
pub fn opencode_capabilities(_cfg: &InstanceConfig) -> Capabilities {
    let mut caps = Capabilities::default();
    caps.sessions.model_switch_in_session = true;
    caps.sessions.provider_switching_via_handoff = true;
    caps.sessions.runtime_mode_switch_in_session = true;
    caps.sessions.pending_requests_survive_restart = true;
    caps.threads.create_empty_thread = true;
    caps.threads.read_snapshot = true;
    caps.threads.rollback = true;
    caps.threads.fork = true;
    caps.threads.fork_from_turn = true;
    caps.threads.native_thread_id = true;
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
    caps.planning.structured_questions = true;
    caps.context.accepts_synthetic_user_context = true;
    caps.context.can_consume_handoff_summaries = true;
    caps.context.delta_handoff = true;
    caps.context.full_thread_handoff = true;
    caps.checkpointing.app_can_checkpoint_filesystem = true;
    caps.checkpointing.nested_scopes = true;
    caps.checkpointing.provider_can_rollback_conversation = true;
    caps.checkpointing.provider_can_read_conversation_snapshot = true;
    caps.identity.thread_ids = RefStrength::Strong;
    caps.identity.turn_ids = RefStrength::Weak;
    caps.identity.item_ids = RefStrength::Strong;
    caps.identity.request_ids = RefStrength::Strong;
    caps.runtime_policy.enforcement = Enforcement::Native;
    caps.runtime_policy.supported_modes = vec![
        RuntimeMode::Ask,
        RuntimeMode::AutoAcceptEdits,
        RuntimeMode::Auto,
        RuntimeMode::FullAccess,
    ];
    caps.runtime_policy.plan_mode_toggle = true;
    caps.extras.background_work = true;
    caps.extras.agent_presets = true;
    caps.extras.model_discovery = true;
    caps
}

/// RFC 0002 "Selection transitions", OpenCode column.
pub fn opencode_transition(from: &Selection, to: &Selection) -> TransitionPlan {
    let mut plans = Vec::new();
    if from.model != to.model {
        // `POST …/model`.
        plans.push(TransitionPlan::ApplyInSession);
    }
    if from.runtime_mode != to.runtime_mode {
        // The mode is the driver's own permission policy.
        plans.push(TransitionPlan::ApplyInSession);
    }
    if from.reasoning_effort != to.reasoning_effort
        || from.service_tier != to.service_tier
        || from.context_window != to.context_window
        || from.agent_preset != to.agent_preset
        || from.interaction_mode != to.interaction_mode
    {
        // The variant, the agent and the plan agent go with each prompt.
        plans.push(TransitionPlan::ApplyOnNextTurn);
    }
    TransitionPlan::combine(plans)
}

/// Waku stores a model as `"provider/model"`; OpenCode wants the two apart,
/// plus the variant that carries the reasoning effort.
fn model_ref(selection: &Selection) -> Option<ModelRef> {
    let (provider_id, id) = selection.model.as_deref()?.split_once('/')?;
    (!provider_id.is_empty() && !id.is_empty()).then(|| ModelRef {
        id: id.to_owned(),
        provider_id: provider_id.to_owned(),
        variant: selection
            .reasoning_effort
            .clone()
            .filter(|variant| !variant.is_empty()),
    })
}

/// The agent a turn runs: plan mode is OpenCode's `plan` agent; otherwise
/// the user's preset, else `build`.
fn agent_for(selection: &Selection) -> String {
    if selection.interaction_mode == InteractionMode::Plan {
        return "plan".to_owned();
    }
    selection
        .agent_preset
        .clone()
        .filter(|preset| !preset.is_empty())
        .unwrap_or_else(|| "build".to_owned())
}

#[derive(Clone, Copy, Debug, Default)]
pub struct OpenCodeDriver;

#[async_trait]
impl DriverV2 for OpenCodeDriver {
    fn kind(&self) -> DriverFamily {
        DriverFamily::OpenCode
    }

    fn capabilities(&self, cfg: &InstanceConfig) -> Capabilities {
        opencode_capabilities(cfg)
    }

    fn plan_selection_transition(
        &self,
        _session_caps: &Capabilities,
        from: &Selection,
        to: &Selection,
    ) -> TransitionPlan {
        opencode_transition(from, to)
    }

    async fn open_session(&self, ctx: SessionContext) -> DriverResult<OpenedSession> {
        let capabilities = self.capabilities(&ctx.instance);
        let base = service_url(&ctx.instance).await?;
        let http = ctx.transports.http("main", base).await?;
        // SUBSCRIBE FIRST: the service can publish a session's first event
        // before the request that creates it has answered.
        let downlink = http.stream(EVENT_ROUTE).await?;
        let api = Api::new(http);
        let windows = api.context_windows().await.unwrap_or_default();
        // The service compares directories by exact string: canonical, no
        // trailing slash, the same string for create and list.
        let requested = ctx.cwd.clone();
        let cwd = tokio::task::spawn_blocking(move || {
            // Not `\\?\`-prefixed on Windows: the service compares strings.
            dunce::canonicalize(&requested).unwrap_or(requested)
        })
        .await
        .unwrap_or_else(|_| ctx.cwd.clone());
        let directory = cwd.to_string_lossy().trim_end_matches('/').to_owned();
        let (events, receiver) = adapter_channel();
        let (commands, inbox) = mpsc::unbounded_channel();
        let shells = Arc::new(AtomicUsize::new(0));
        let mut normalizer = Normalizer::new(
            ctx.ids.clone(),
            ctx.clock.clone(),
            ctx.provider_session_id,
            ctx.selection.runtime_mode,
        );
        normalizer.context_windows = windows;
        let actor = SessionActor {
            api,
            directory,
            ctx_ids: ctx.ids.clone(),
            raw_log: ctx.raw_log.clone(),
            provider_session_id: ctx.provider_session_id,
            thread_id: ctx.thread_id,
            instance_id: ctx.instance.instance_id.clone(),
            clock: ctx.clock.clone(),
            norm: normalizer,
            events: Some(events),
            native: None,
            thread: None,
            agent: None,
            commands_cache: None,
            shells: shells.clone(),
        };
        tokio::spawn(actor.run(inbox, ctx.shutdown.clone(), downlink));
        Ok(OpenedSession {
            session: Arc::new(OpenCodeSession {
                commands,
                capabilities: capabilities.clone(),
                shells,
            }),
            events: receiver,
            capabilities,
        })
    }
}

/// The service's base URL with its Basic credential in the user info. An
/// instance may name one in `extra.serviceUrl` (replay, a service the user
/// runs elsewhere); otherwise the adopted background service is used.
async fn service_url(instance: &InstanceConfig) -> DriverResult<Url> {
    if let Some(url) = instance.extra.get("serviceUrl").and_then(Value::as_str) {
        return Url::parse(url).map_err(|error| DriverError::Protocol {
            reason: super::v2::ProtocolReason::Decode,
            detail: format!("invalid serviceUrl: {error}"),
        });
    }
    let binary = instance.binary.clone();
    let url = tokio::task::spawn_blocking(move || {
        crate::opencode_service::shared(&binary).and_then(|service| service.base_url())
    })
    .await
    .map_err(|error| DriverError::SessionLost {
        detail: error.to_string(),
    })?;
    url.map_err(|error| {
        DriverError::Provider(
            ProviderFailure::new(FailureClass::TransportError, error.to_string())
                .with_code("service_unavailable"),
        )
    })
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

struct OpenCodeSession {
    commands: mpsc::UnboundedSender<Command>,
    capabilities: Capabilities,
    shells: Arc<AtomicUsize>,
}

impl OpenCodeSession {
    async fn call<T>(&self, command: impl FnOnce(Reply<T>) -> Command) -> DriverResult<T> {
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(command(reply))
            .map_err(|_| DriverError::SessionLost {
                detail: "the OpenCode session closed".to_owned(),
            })?;
        answer.await.map_err(|_| DriverError::SessionLost {
            detail: "the OpenCode session closed".to_owned(),
        })?
    }
}

#[async_trait]
impl DriverSession for OpenCodeSession {
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
        self.shells.load(Ordering::Acquire) > 0
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
    api: Api,
    directory: String,
    ctx_ids: Arc<dyn super::v2::IdAllocator>,
    raw_log: Arc<dyn super::v2::RawFrameLog>,
    provider_session_id: ProviderSessionId,
    thread_id: ThreadId,
    instance_id: ProviderInstanceId,
    clock: Arc<dyn super::v2::Clock>,
    norm: Normalizer,
    events: Option<AdapterEventSender>,
    /// The native session this actor drives (changes on a rollback).
    native: Option<String>,
    thread: Option<ProviderThreadRef>,
    /// The agent the native session runs now.
    agent: Option<String>,
    commands_cache: Option<HashSet<String>>,
    shells: Arc<AtomicUsize>,
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

    /// The downlink lives here rather than on the actor: a stream is `Send`
    /// but not `Sync`, and the actor's `&self` methods await.
    async fn run(
        mut self,
        mut inbox: mpsc::UnboundedReceiver<Command>,
        shutdown: super::v2::Shutdown,
        downlink: Downlink,
    ) {
        let mut downlink = Some(downlink);
        let mut backoff = RESUBSCRIBE_MIN;
        let mut resubscribe_at: Option<tokio::time::Instant> = None;
        loop {
            let tick = self.norm.has_pending_snapshots();
            tokio::select! {
                biased;
                command = inbox.recv() => {
                    let Some(command) = command else { break };
                    if !self.handle(command).await {
                        break;
                    }
                }
                event = next_event(&mut downlink) => {
                    match event {
                        Some(Ok(envelope)) => {
                            backoff = RESUBSCRIBE_MIN;
                            self.on_envelope(envelope).await;
                        }
                        // The stream blinked. Reconnection is the service's
                        // job, and a break is never `SessionLost`.
                        Some(Err(error)) => {
                            self.note(&format!("event stream ended: {error}"));
                            downlink = None;
                            resubscribe_at = Some(tokio::time::Instant::now());
                        }
                        None => {
                            downlink = None;
                            resubscribe_at = Some(tokio::time::Instant::now());
                        }
                    }
                }
                () = sleep_until(resubscribe_at), if resubscribe_at.is_some() => {
                    resubscribe_at = None;
                    match self.api.transport().stream(EVENT_ROUTE).await {
                        Ok(reopened) => {
                            downlink = Some(reopened);
                            self.reconcile().await;
                        }
                        Err(error) => {
                            self.note(&format!("resubscribe failed: {error}"));
                            resubscribe_at = Some(tokio::time::Instant::now() + backoff);
                            backoff = (backoff * 2).min(RESUBSCRIBE_MAX);
                        }
                    }
                }
                () = tokio::time::sleep(Duration::from_millis(16)), if tick => {
                    let due = self.norm.due();
                    self.emit(due).await;
                }
                () = shutdown.cancelled() => break,
            }
        }
        // Unsubscribe only: the adopted service is never touched.
        drop(downlink);
        self.events = None;
    }

    async fn on_envelope(&mut self, envelope: Value) {
        let kind = envelope
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let Some(native) = self.native.as_deref() else {
            return;
        };
        if routed_session(&envelope) != Some(native) {
            return;
        }
        let data = envelope.get("data").cloned().unwrap_or(Value::Null);
        let (events, effects) = self.norm.on_event(&kind, &data);
        if events.is_empty() && effects.is_empty() && !is_known(&kind) {
            self.raw_log
                .record("main", FrameDirection::Inbound, &envelope.to_string());
        }
        self.sync_shells();
        self.emit(events).await;
        for effect in effects {
            match effect {
                Effect::AutoReply { request } => {
                    let session = native.to_owned();
                    if let Err(error) = self
                        .api
                        .reply_permission(&session, &request, PermissionReply::Once)
                        .await
                    {
                        self.note(&format!("automatic permission reply failed: {error:?}"));
                    }
                }
                Effect::Interrupt => {
                    let session = native.to_owned();
                    let _ = self.api.interrupt(&session).await;
                }
            }
        }
    }

    fn sync_shells(&self) {
        self.shells.store(self.norm.shells.len(), Ordering::Release);
    }

    /// After a resubscribe: requests raised and outcomes reached while the
    /// stream was down.
    async fn reconcile(&mut self) {
        let Some(native) = self.native.clone() else {
            return;
        };
        if self.norm.turn.is_none() {
            return;
        }
        let mut events = Vec::new();
        if let Ok(permissions) = self.api.pending_permissions(&native).await {
            for permission in permissions {
                let (more, effects) = self.norm.permission(&permission);
                events.extend(more);
                for effect in effects {
                    if let Effect::AutoReply { request } = effect {
                        let _ = self
                            .api
                            .reply_permission(&native, &request, PermissionReply::Once)
                            .await;
                    }
                }
            }
        }
        if let Ok(forms) = self.api.pending_forms(&native).await {
            for form in forms {
                events.extend(self.norm.form(&form));
            }
        }
        self.emit(events).await;
        // A turn whose execution ended while the stream was down never sees
        // its outcome event: settle it from the session's own outcome.
        let active = self
            .api
            .transport()
            .request(super::v2::HttpMethod::Get, "/api/session/active", None)
            .await
            .ok()
            .and_then(|response| response.body.get("data").cloned());
        let running = active
            .as_ref()
            .and_then(Value::as_object)
            .is_none_or(|active| active.contains_key(&native));
        if running {
            return;
        }
        let outcome = match self.api.get_session(&native).await {
            Ok(session) => match session.get("outcome").and_then(Value::as_str) {
                Some("failed") => TurnOutcome::Failed {
                    failure: normalize::execution_failure(None, None),
                },
                Some("interrupted") => TurnOutcome::Interrupted,
                _ => TurnOutcome::Completed,
            },
            Err(_) => TurnOutcome::Completed,
        };
        let events = self.norm.finish_turn(outcome, ThreadDisposition::Reusable);
        self.emit(events).await;
    }

    fn thread_ref(&self, provider_thread_id: ProviderThreadId, native: &str) -> ProviderThreadRef {
        ProviderThreadRef {
            provider_thread_id,
            native: NativeRef::strong(native),
            cursor: Some(ProviderResumeCursor::OpenCode {
                session_id: native.to_owned(),
                directory: Some(self.directory.clone()),
            }),
        }
    }

    async fn handle(&mut self, command: Command) -> bool {
        match command {
            Command::Ensure(req, reply) => {
                let result = self.ensure(req).await;
                let _ = reply.send(result);
            }
            Command::Resume(req, reply) => {
                let result = self.resume(req).await;
                let _ = reply.send(result);
            }
            Command::Start(req, reply) => {
                let result = self.start(*req).await;
                let _ = reply.send(result);
            }
            Command::Steer(req, reply) => {
                let result = self.steer(req).await;
                let _ = reply.send(result);
            }
            Command::Interrupt(req, reply) => {
                let result = self.interrupt(req).await;
                let _ = reply.send(result);
            }
            Command::Respond(req, reply) => {
                let result = self.respond(req).await;
                let _ = reply.send(result);
            }
            Command::Snapshot(thread, reply) => {
                let result = self.snapshot(&thread).await;
                let _ = reply.send(result);
            }
            Command::Rollback(req, reply) => {
                let result = self.rollback(req).await;
                let _ = reply.send(result);
            }
            Command::Fork(req, reply) => {
                let result = self.fork(req).await;
                let _ = reply.send(result);
            }
            Command::Apply(selection, reply) => {
                let result = self.apply(&selection).await;
                let _ = reply.send(result);
            }
            Command::Close(reply) => {
                let _ = reply.send(());
                return false;
            }
        }
        true
    }

    async fn ensure(&mut self, req: EnsureThread) -> DriverResult<ProviderThreadRef> {
        let agent = self.resolve_agent(&req.selection).await;
        let model = model_ref(&req.selection);
        let created = self
            .api
            .create_session(&self.directory, Some(&agent), model.as_ref())
            .await
            .map_err(|error| error.into_driver("create a session"))?;
        let native = created
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| DriverError::Protocol {
                reason: super::v2::ProtocolReason::UnexpectedFrame,
                detail: "OpenCode returned no session id".to_owned(),
            })?
            .to_owned();
        self.agent = Some(agent);
        self.norm.model = model.or_else(|| {
            serde_json::from_value(created.get("model").cloned().unwrap_or(Value::Null)).ok()
        });
        self.norm.mode = req.selection.runtime_mode;
        let provider_thread_id = match &req.existing {
            Some(existing) => existing.provider_thread_id,
            None => ProviderThreadId(self.ctx_ids.bind(
                self.provider_session_id.0,
                EntityKind::ProviderThread,
                &native,
            )),
        };
        let reference = self.thread_ref(provider_thread_id, &native);
        self.native = Some(native);
        self.norm.provider_thread = Some(reference.provider_thread_id);
        self.thread = Some(reference.clone());
        Ok(reference)
    }

    async fn resume(&mut self, req: ResumeThread) -> DriverResult<ProviderThreadRef> {
        let native = match &req.thread.cursor {
            Some(ProviderResumeCursor::OpenCode { session_id, .. }) if !session_id.is_empty() => {
                session_id.clone()
            }
            _ => req.thread.native.id.clone(),
        };
        let session = match self.api.get_session(&native).await {
            Ok(session) => session,
            // A cursor can outlive its session (the user's own client can
            // delete it): start a fresh one for the same provider thread.
            Err(error) if error.is_not_found() => {
                return self
                    .ensure(EnsureThread {
                        thread_id: self.thread_id,
                        title: None,
                        selection: req.selection,
                        existing: Some(req.thread),
                    })
                    .await;
            }
            Err(error) => return Err(error.into_driver("read the session")),
        };
        self.agent = session
            .get("agent")
            .and_then(Value::as_str)
            .map(str::to_owned);
        self.norm.model =
            serde_json::from_value(session.get("model").cloned().unwrap_or(Value::Null)).ok();
        self.norm.mode = req.selection.runtime_mode;
        // Requests still pending survive a restart: they are surfaced with
        // the next turn of this session.
        if let Ok(permissions) = self.api.pending_permissions(&native).await {
            self.norm.stranded.extend(permissions);
        }
        if let Ok(forms) = self.api.pending_forms(&native).await {
            self.norm.stranded.extend(forms);
        }
        let reference = self.thread_ref(req.thread.provider_thread_id, &native);
        self.native = Some(native);
        self.norm.provider_thread = Some(reference.provider_thread_id);
        self.thread = Some(reference.clone());
        Ok(reference)
    }

    /// The user's preset if the service still lists it as a selectable
    /// primary agent, else `build`; plan mode is the `plan` agent.
    async fn resolve_agent(&self, selection: &Selection) -> String {
        let agent = agent_for(selection);
        if agent == "build" || agent == "plan" {
            return agent;
        }
        match self.api.selectable_agents(&self.directory).await {
            Ok(agents) if !agents.is_empty() && !agents.contains(&agent) => "build".to_owned(),
            _ => agent,
        }
    }

    fn bound_session(&mut self, thread: &ProviderThreadRef) -> DriverResult<String> {
        if thread.is_bound() && self.native.as_deref() != Some(thread.native.id.as_str()) {
            // A different native session than the one this actor follows
            // (a replacement after a rollback, or a fresh bind).
            self.native = Some(thread.native.id.clone());
            self.thread = Some(thread.clone());
        }
        self.native
            .clone()
            .ok_or(DriverError::NotReady(NotReadyReason::ThreadMissing))
    }

    async fn sync_selection(&mut self, session: &str, selection: &Selection) -> DriverResult<()> {
        let agent = agent_for(selection);
        if self.agent.as_deref() != Some(agent.as_str()) {
            let agent = self.resolve_agent(selection).await;
            if self.agent.as_deref() != Some(agent.as_str()) {
                self.api
                    .switch_agent(session, &agent)
                    .await
                    .map_err(|error| error.into_driver("switch the agent"))?;
                self.agent = Some(agent);
            }
        }
        if let Some(model) = model_ref(selection)
            && self.norm.model.as_ref() != Some(&model)
        {
            self.api
                .switch_model(session, &model)
                .await
                .map_err(|error| error.into_driver("switch the model"))?;
            self.norm.model = Some(model);
        }
        self.norm.mode = selection.runtime_mode;
        Ok(())
    }

    async fn is_command(&mut self, name: &str) -> bool {
        if self.commands_cache.is_none() {
            let names = self
                .api
                .command_names(&self.directory)
                .await
                .unwrap_or_default();
            self.commands_cache = Some(names.into_iter().collect());
        }
        self.commands_cache
            .as_ref()
            .is_some_and(|names| names.contains(name))
    }

    async fn start(&mut self, req: TurnStart) -> DriverResult<TurnAccepted> {
        if self.norm.turn.is_some() {
            return Err(DriverError::NotReady(NotReadyReason::PreviousTurnActive));
        }
        let session = self.bound_session(&req.thread)?;
        self.sync_selection(&session, &req.selection).await?;
        let text = turn_text(&req.app_context, &req.message.text);
        let invocation = match &req.intent {
            TurnIntent::SlashCommand { name } => Some((name.clone(), req.message.text.clone())),
            _ => slash_invocation(&req.message.text),
        };
        let native_turn = match invocation {
            Some((name, arguments)) if self.is_command(&name).await => {
                self.api
                    .command(&session, &name, &arguments)
                    .await
                    .map_err(|error| error.into_driver("run the command"))?;
                None
            }
            _ => {
                let (inbox, _) = self
                    .api
                    .prompt(&session, &text, None)
                    .await
                    .map_err(|error| error.into_driver("start the turn"))?;
                Some(inbox)
            }
        };
        let pt = req.thread.provider_thread_id;
        let reference = match native_turn {
            Some(native) => ProviderTurnRef {
                provider_turn_id: ProviderTurnId(self.ctx_ids.bind(
                    pt.0,
                    EntityKind::ProviderTurn,
                    &native,
                )),
                native: NativeRef::weak(native),
                ordinal: req.provider_turn_ordinal,
            },
            None => ProviderTurnRef {
                provider_turn_id: ProviderTurnId(self.ctx_ids.bind_ordinal(
                    pt.0,
                    EntityKind::ProviderTurn,
                    u64::from(req.provider_turn_ordinal),
                )),
                native: NativeRef {
                    id: String::new(),
                    strength: RefStrength::None,
                },
                ordinal: req.provider_turn_ordinal,
            },
        };
        let events = self.norm.begin_turn(OpenTurn {
            run: req.run,
            reference: reference.clone(),
            provider_thread_id: pt,
            started_at: self.clock.now(),
            interrupted: false,
            usage: None,
            step_error: None,
        });
        self.emit(events).await;
        Ok(TurnAccepted {
            provider_turn: Some(reference),
        })
    }

    async fn steer(&mut self, req: Steer) -> DriverResult<()> {
        let Some(turn) = self.norm.turn.as_ref() else {
            return Err(DriverError::SteerRejected {
                reason: SteerRejectReason::NoActiveTurn,
            });
        };
        if turn.run.attempt_id != req.run.attempt_id || turn.interrupted {
            return Err(DriverError::SteerRejected {
                reason: SteerRejectReason::TurnSettled,
            });
        }
        let session = self.bound_session(&req.thread)?;
        let (inbox, delivery) = self
            .api
            .prompt(&session, &req.message.text, Some(Delivery::Steer))
            .await
            .map_err(|_| DriverError::SteerRejected {
                reason: SteerRejectReason::ProviderRefused,
            })?;
        // A service that queued the message anyway is promoted, so "steer"
        // means the same thing on both paths.
        if delivery
            .as_deref()
            .is_some_and(|delivery| delivery != "steer")
        {
            let _ = self.api.steer_inbox(&session, &inbox).await;
        }
        Ok(())
    }

    async fn interrupt(&mut self, req: Interrupt) -> DriverResult<()> {
        let Some(turn) = self.norm.turn.as_mut() else {
            return Ok(());
        };
        if turn.run.attempt_id != req.run.attempt_id {
            return Ok(());
        }
        turn.interrupted = true;
        let Some(session) = self.native.clone() else {
            return Ok(());
        };
        match self.api.interrupt(&session).await {
            // Its own `interrupted` outcome is the terminal.
            Ok(true) => Ok(()),
            // Nothing was executing (the prompt still waits in the inbox, or
            // the run is suspended by a service shutdown): the turn ends now,
            // and the run is stopped when it next starts.
            Ok(false) => {
                self.norm.stop_pending = true;
                let events = self
                    .norm
                    .finish_turn(TurnOutcome::Cancelled, ThreadDisposition::Reusable);
                self.norm.stop_pending = true;
                self.emit(events).await;
                Ok(())
            }
            Err(ApiError::Transport(_)) => {
                self.norm.stop_pending = true;
                let events = self
                    .norm
                    .finish_turn(TurnOutcome::Interrupted, ThreadDisposition::Reusable);
                self.norm.stop_pending = true;
                self.emit(events).await;
                Ok(())
            }
            Err(error) => Err(error.into_driver("stop the turn")),
        }
    }

    async fn respond(&mut self, req: RuntimeResponse) -> DriverResult<()> {
        let Some(session) = self.native.clone() else {
            return Err(DriverError::NotReady(NotReadyReason::ThreadMissing));
        };
        let native = req.native.id.clone();
        let fields = self
            .norm
            .pending
            .get(&native)
            .map(|pending| pending.fields.clone());
        let Some(fields) = fields else {
            // Already answered (here or by another client).
            return Ok(());
        };
        let mut events = Vec::new();
        match (fields, req.reply) {
            (None, RuntimeReply::Decision { option_id }) => {
                for (request, option) in crate::driver::support::permission_responses(
                    &mut self.norm.permissions,
                    &native,
                    &option_id,
                ) {
                    let reply = if option == normalize::OPTION_REJECT {
                        PermissionReply::Reject
                    } else {
                        PermissionReply::Once
                    };
                    self.api
                        .reply_permission(&session, &request, reply)
                        .await
                        .map_err(|error| error.into_driver("answer the permission"))?;
                    events.extend(self.norm.resolve(&request, RequestStatus::Resolved));
                }
            }
            (None, RuntimeReply::Dismiss) => {
                self.api
                    .reply_permission(&session, &native, PermissionReply::Reject)
                    .await
                    .map_err(|error| error.into_driver("answer the permission"))?;
                events.extend(self.norm.resolve(&native, RequestStatus::Resolved));
            }
            (Some(fields), RuntimeReply::Answers(answers)) => {
                let answer = form_answer(&fields, &answers);
                self.api
                    .reply_form(&session, &native, &answer)
                    .await
                    .map_err(|error| error.into_driver("answer the question"))?;
                events.extend(self.norm.resolve(&native, RequestStatus::Resolved));
            }
            _ => {
                return Err(DriverError::Unsupported {
                    operation: "respond",
                });
            }
        }
        self.emit(events).await;
        Ok(())
    }

    /// The native turn boundaries of a session: the id of the user message
    /// that opens each turn, oldest first (`opencode_session`'s rule).
    async fn boundaries(&self, session: &str) -> DriverResult<Vec<String>> {
        let messages = self
            .api
            .messages(session)
            .await
            .map_err(|error| error.into_driver("list the messages"))?;
        let mut boundaries = crate::opencode_session::TurnBoundaries::default();
        Ok(messages
            .into_iter()
            .filter_map(|message| {
                let message = serde_json::from_value::<MessageInfo>(message).ok()?;
                boundaries.observe(&message).map(str::to_owned)
            })
            .collect())
    }

    async fn fork_session(&self, session: &str, before: Option<&str>) -> DriverResult<String> {
        let forked = self
            .api
            .fork(session, before)
            .await
            .map_err(|error| error.into_driver("fork the session"))?;
        forked
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
            .map(str::to_owned)
            .ok_or_else(|| DriverError::Protocol {
                reason: super::v2::ProtocolReason::UnexpectedFrame,
                detail: "OpenCode returned no forked session id".to_owned(),
            })
    }

    /// Replaced: `POST …/fork { before }` at the first removed turn.
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
        let first_removed = req
            .turns
            .get(kept)
            .map(|turn| turn.native.id.clone())
            .filter(|id| !id.is_empty());
        let before = match first_removed {
            Some(id) => Some(id),
            None => {
                let boundaries = self.boundaries(&session).await?;
                let kept = boundaries
                    .len()
                    .saturating_sub(req.turns_to_remove as usize);
                boundaries.get(kept).cloned()
            }
        };
        let forked = self.fork_session(&session, before.as_deref()).await?;
        let reference = self.thread_ref(
            ProviderThreadId(self.ctx_ids.bind(
                req.thread.provider_thread_id.0,
                EntityKind::ProviderThread,
                &forked,
            )),
            &forked,
        );
        self.native = Some(forked);
        self.norm.provider_thread = Some(reference.provider_thread_id);
        self.thread = Some(reference.clone());
        Ok(RollbackOutcome {
            thread: reference,
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
        let session = match &req.source.cursor {
            Some(ProviderResumeCursor::OpenCode { session_id, .. }) if !session_id.is_empty() => {
                session_id.clone()
            }
            _ => req.source.native.id.clone(),
        };
        let before = match &req.at {
            ForkPoint::Tip => None,
            ForkPoint::AfterProviderTurn {
                turns_to_remove: 0, ..
            } => None,
            ForkPoint::AfterProviderTurn {
                turn,
                turns_to_remove,
            } => {
                let boundaries = self.boundaries(&session).await?;
                let after_kept = boundaries
                    .iter()
                    .position(|id| *id == turn.native.id)
                    .map(|index| index + 1)
                    .unwrap_or_else(|| boundaries.len().saturating_sub(*turns_to_remove as usize));
                boundaries.get(after_kept).cloned()
            }
            ForkPoint::SubagentThread(_) => {
                return Err(DriverError::Unsupported {
                    operation: "fork_from_subagent_thread",
                });
            }
        };
        let forked = self.fork_session(&session, before.as_deref()).await?;
        Ok(self.thread_ref(
            ProviderThreadId(self.ctx_ids.bind(
                req.source.provider_thread_id.0,
                EntityKind::ProviderThread,
                &forked,
            )),
            &forked,
        ))
    }

    async fn apply(&mut self, selection: &Selection) -> DriverResult<()> {
        if let Some(session) = self.native.clone()
            && let Some(model) = model_ref(selection)
            && self.norm.model.as_ref() != Some(&model)
        {
            self.api
                .switch_model(&session, &model)
                .await
                .map_err(|error| error.into_driver("switch the model"))?;
            self.norm.model = Some(model);
        }
        self.norm.mode = selection.runtime_mode;
        Ok(())
    }

    /// The conversation as stored: user and assistant text, turn rows from
    /// the native boundaries, and the requests still pending.
    async fn snapshot(&self, thread: &ProviderThreadRef) -> DriverResult<ThreadSnapshot> {
        let session = thread.native.id.clone();
        let messages = self
            .api
            .messages(&session)
            .await
            .map_err(|error| error.into_driver("list the messages"))?;
        let now = self.clock.now();
        let pt = thread.provider_thread_id;
        let mut boundaries = crate::opencode_session::TurnBoundaries::default();
        let mut turns = Vec::new();
        let mut rows = Vec::new();
        for message in messages {
            let Ok(info) = serde_json::from_value::<MessageInfo>(message) else {
                continue;
            };
            if let Some(opening) = boundaries.observe(&info) {
                let ordinal = turns.len() as u32 + 1;
                turns.push(ProviderTurn {
                    id: ProviderTurnId(self.ctx_ids.bind(pt.0, EntityKind::ProviderTurn, opening)),
                    provider_thread_id: pt,
                    node_id: NodeId(self.ctx_ids.bind(pt.0, EntityKind::Node, opening)),
                    run_attempt_id: None,
                    native_turn_ref: Some(NativeRef::weak(opening).to_provider_ref(DRIVER)),
                    ordinal,
                    status: ProviderTurnStatus::Completed,
                    started_at: None,
                    completed_at: None,
                    token_usage: None,
                    turn_token_usage: None,
                });
            }
            let (id, role, text) = match &info {
                MessageInfo::User { id, text, .. } => (id, MessageRole::User, text.clone()),
                MessageInfo::Assistant { id, content, .. } => (
                    id,
                    MessageRole::Assistant,
                    content
                        .iter()
                        .filter_map(|part| match part {
                            crate::opencode_api::AssistantContent::Text { text, .. } => {
                                Some(text.as_str())
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join(""),
                ),
                _ => continue,
            };
            rows.push(Message {
                id: MessageId(self.ctx_ids.bind(pt.0, EntityKind::Message, id)),
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
            messages: rows,
            pending_requests: self
                .norm
                .pending
                .values()
                .map(|pending| pending.request.clone())
                .collect(),
        })
    }
}

async fn next_event(downlink: &mut Option<Downlink>) -> Option<std::io::Result<Value>> {
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

/// The native session an `/api/event` envelope belongs to.
///
/// Demultiplexing is a security boundary: the stream carries the user's own
/// TUI sessions and every other Waku task. `tui.*` are remote-control
/// commands broadcast to every client and never reach a session, and
/// `form.created` is the one event whose session id is nested (a uniform
/// reader would find none there).
fn routed_session(envelope: &Value) -> Option<&str> {
    let kind = envelope.get("type").and_then(Value::as_str)?;
    if kind.starts_with("tui.") {
        return None;
    }
    let data = envelope.get("data")?;
    if kind == "form.created" {
        data.pointer("/form/sessionID").and_then(Value::as_str)
    } else {
        data.get("sessionID").and_then(Value::as_str)
    }
}

/// Event families that are known and deliberately not normalized; anything
/// else goes to the raw log.
fn is_known(kind: &str) -> bool {
    kind.starts_with("session.")
        || kind.starts_with("permission.")
        || kind.starts_with("form.")
        || kind == "server.connected"
}

/// Synthetic-user context goes in the shared envelope (port step 11):
/// OpenCode has no developer or system channel per prompt.
fn turn_text(context: &[ContextBlock], text: &str) -> String {
    wrap_synthetic_context(context, text)
}

/// `/name arguments` → `(name, arguments)`.
fn slash_invocation(text: &str) -> Option<(String, String)> {
    let invocation = text.strip_prefix('/')?;
    let (name, arguments) = invocation
        .split_once(char::is_whitespace)
        .unwrap_or((invocation, ""));
    (!name.is_empty()).then(|| (name.to_owned(), arguments.trim().to_owned()))
}

/// A form answer keyed by each field's own `key`; the card carries option
/// labels, the service wants option values.
fn form_answer(fields: &[FormField], answers: &[UserInputAnswer]) -> FormAnswer {
    let mut answer = FormAnswer::new();
    for field in fields {
        let (key, options): (&str, &[crate::opencode_api::FormOption]) = match field {
            FormField::String { key, options, .. } => (key, options.as_deref().unwrap_or_default()),
            FormField::Multiselect { key, options, .. } => (key, options),
            FormField::Number { key, .. }
            | FormField::Integer { key, .. }
            | FormField::Boolean { key, .. }
            | FormField::External { key, .. } => (key, &[]),
            FormField::Unknown => continue,
        };
        let Some(given) = answers
            .iter()
            .find(|answer| answer.question_id == key)
            .map(|answer| answer.answers.as_slice())
        else {
            continue;
        };
        let resolve = |value: &String| -> String {
            options
                .iter()
                .find(|option| &option.label == value)
                .map(|option| option.value.clone())
                .unwrap_or_else(|| value.clone())
        };
        let value = match field {
            FormField::Multiselect { .. } => FormValue::List(given.iter().map(resolve).collect()),
            FormField::Boolean { .. } => FormValue::Bool(matches!(
                given.first().map(String::as_str),
                Some("true" | "yes" | "1")
            )),
            FormField::Number { .. } | FormField::Integer { .. } => given
                .first()
                .and_then(|value| value.parse::<f64>().ok())
                .map(FormValue::Number)
                .unwrap_or_else(|| FormValue::Text(given.first().cloned().unwrap_or_default())),
            _ => FormValue::Text(given.first().map(resolve).unwrap_or_default()),
        };
        answer.insert(key.to_owned(), value);
    }
    answer
}

#[cfg(test)]
mod tests;

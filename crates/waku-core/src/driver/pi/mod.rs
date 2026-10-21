//! Pi and Oh My Pi on `DriverV2` (RFC 0002 "Pi and Oh My Pi", #50).
//!
//! `pi --mode rpc` (or `omp --mode rpc`) serves one session per process over
//! newline-delimited JSON: commands carry an `id` and get a `response` with
//! it, and the agent streams events in between. One driver serves both
//! flavors; [`PiFlavor`] carries their differences.
//!
//! The process starts in `open_session` (channel `main`): Oh My Pi first
//! negotiates protocol v2 (chunked oversize frames), then `get_state`
//! reports the session it created. `ensure_thread` binds that session (or
//! asks for a `new_session`), `resume_thread` switches to the cursor's
//! session file. A turn is a `prompt`; the agent's settle event
//! (`agent_settled`, or Oh My Pi's terminal `agent_end`) ends it. Rollback
//! forks or branches the live session at the first removed user entry and
//! replaces the provider thread; a fork copies the source session with a
//! throwaway agent (`--fork <file>` in the session's directory, channel
//! `fork-1`), so the live session is never touched.
//!
//! Approvals: Pi has no permission system, so in supervised modes the
//! session loads Waku's extension, whose `tool_call` hook asks through a
//! `confirm` dialog; Oh My Pi runs with its own `--approval-mode` and asks
//! through a `select`. Every other extension dialog (`select`, `input`,
//! `editor`) is a structured question instead of being cancelled.

mod normalize;
pub mod protocol;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::oneshot;
use waku_protocol::orchestration::{self as orch, *};

use self::normalize::{Normalizer, Output, Scope};
use self::protocol::{ChunkAssembly, SessionState, Supervision};
pub use self::protocol::{OHMYPI_PROVIDER, PI_PROTOCOL, PI_PROVIDER, PiFlavor};
use crate::driver::v2::{
    AdapterEvent, AdapterEventSender, Clock, ContextBlock, DriverError, DriverFamily, DriverResult,
    DriverSession, DriverV2, EnsureThread, EntityKind, ForkPoint, ForkRequest, FrameDirection,
    IdAllocator, InstanceConfig, Interrupt, NativeRef, NotReadyReason, OpenedSession, ProcessSpec,
    ProtocolReason, ProviderThreadRef, RawFrameLog, ResumeThread, Rollback, RollbackOutcome,
    RollbackTarget, RuntimeResponse, Selection, SessionContext, Steer, SteerRejectReason,
    ThreadSnapshot, TransitionPlan, TransportFactory, TurnAccepted, TurnOutcome, TurnStart,
    adapter_channel, wrap_synthetic_context,
};

/// A command against the running agent.
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
/// The handshake races the agent's own startup: Pi loads extensions and
/// resources and may refresh its model catalog (budgeted 15 s on its own)
/// before it reads stdin.
const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(30);
/// A fork starts a whole second agent.
const FORK_TIMEOUT: Duration = Duration::from_secs(30);
/// After an abort, how long the agent has to settle the turn before the
/// driver settles it itself.
const INTERRUPT_GRACE: Duration = Duration::from_secs(10);

/// The capabilities this driver really has (RFC 0002 matrix, Pi column, as
/// amended by #50). Both flavors declare the same set: Pi through Waku's
/// approval extension, Oh My Pi through its own approval modes.
pub fn pi_capabilities(_flavor: PiFlavor) -> Capabilities {
    let mut caps = Capabilities::default();
    caps.sessions.model_switch_in_session = true;
    caps.sessions.provider_switching_via_handoff = true;
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
    caps.turns.terminal_status_quality = Quality::Weak;
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
    caps.identity.turn_ids = RefStrength::None;
    caps.identity.item_ids = RefStrength::Weak;
    caps.identity.request_ids = RefStrength::Strong;
    caps.runtime_policy.enforcement = Enforcement::Native;
    caps.runtime_policy.supported_modes = vec![
        RuntimeMode::Ask,
        RuntimeMode::AutoAcceptEdits,
        RuntimeMode::FullAccess,
    ];
    caps.extras.model_discovery = true;
    caps
}

/// An instance of `pi` found on the search path.
pub fn pi_instance() -> InstanceConfig {
    let mut cfg = InstanceConfig::new(PI_PROVIDER, orch::DriverKind::Pi);
    cfg.binary = "pi".into();
    cfg
}

/// An instance of Oh My Pi (`omp`) found on the search path.
pub fn ohmypi_instance() -> InstanceConfig {
    let mut cfg = InstanceConfig::new(OHMYPI_PROVIDER, orch::DriverKind::OhMyPi);
    cfg.binary = "omp".into();
    cfg
}

/// The instance replay and the recorder use: the approval extension is
/// written under the temp directory, never the user's home.
pub fn pi_replay_instance(flavor: PiFlavor) -> InstanceConfig {
    let mut cfg = match flavor {
        PiFlavor::Pi => pi_instance(),
        PiFlavor::OhMyPi => ohmypi_instance(),
    };
    cfg.extra = json!({
        "extensionDir": std::env::temp_dir().join("waku-replay-pi").display().to_string()
    });
    cfg
}

/// The Pi family's driver (Pi and Oh My Pi; `InstanceConfig::provider`
/// picks the flavor).
#[derive(Clone, Copy, Debug, Default)]
pub struct PiDriver;

impl PiDriver {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl DriverV2 for PiDriver {
    fn kind(&self) -> DriverFamily {
        DriverFamily::Pi
    }

    fn capabilities(&self, cfg: &InstanceConfig) -> Capabilities {
        pi_capabilities(PiFlavor::from_instance(cfg))
    }

    fn plan_selection_transition(
        &self,
        _session_caps: &Capabilities,
        from: &Selection,
        to: &Selection,
    ) -> TransitionPlan {
        let mut plans = Vec::new();
        // `set_model` and `set_thinking_level` reach the live session.
        if from.model != to.model || from.reasoning_effort != to.reasoning_effort {
            plans.push(TransitionPlan::ApplyInSession);
        }
        if from.service_tier != to.service_tier
            || from.context_window != to.context_window
            || from.agent_preset != to.agent_preset
        {
            plans.push(TransitionPlan::ApplyOnNextTurn);
        }
        // The approval policy is fixed at launch (the extension's
        // environment, Oh My Pi's `--approval-mode`).
        if from.runtime_mode != to.runtime_mode {
            plans.push(TransitionPlan::RestartSession);
        }
        if from.interaction_mode != to.interaction_mode {
            plans.push(TransitionPlan::Reject {
                reason: "Pi has no plan mode".to_owned(),
            });
        }
        TransitionPlan::combine(plans)
    }

    async fn open_session(&self, ctx: SessionContext) -> DriverResult<OpenedSession> {
        let flavor = PiFlavor::from_instance(&ctx.instance);
        let capabilities = pi_capabilities(flavor);
        let supervision = Supervision::of(ctx.selection.runtime_mode);
        let extension = if flavor == PiFlavor::Pi && supervision != Supervision::FullAccess {
            let dir = protocol::extension_dir(&ctx.instance);
            let path = tokio::task::spawn_blocking(move || protocol::materialize_extension(&dir))
                .await
                .map_err(|error| DriverError::Io(std::io::Error::other(error)))??;
            Some(path)
        } else {
            None
        };
        let (args, env) =
            protocol::launch(flavor, supervision, extension.as_deref(), &ctx.instance);
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
        let shared = Arc::new(Shared {
            flavor,
            normalizer: Mutex::new(Normalizer::new(Scope {
                flavor,
                session_id: ctx.provider_session_id,
                ids: ctx.ids.clone(),
                clock: ctx.clock.clone(),
            })),
            session_id: ctx.provider_session_id,
            ids: ctx.ids.clone(),
            clock: ctx.clock.clone(),
            raw_log: ctx.raw_log.clone(),
            transports: ctx.transports.clone(),
            instance: ctx.instance,
            cwd: ctx.cwd,
            stdin: tokio::sync::Mutex::new(Some(transport.stdin)),
            next_request: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
            accept: Mutex::new(None),
            events: Mutex::new(Some(events)),
            native: Mutex::new(Native::default()),
            selection: Mutex::new(ctx.selection.clone()),
            dead: AtomicBool::new(false),
            capabilities: capabilities.clone(),
        });
        drain_stderr(transport.stderr, ctx.raw_log.clone());
        tokio::spawn(read_loop(shared.clone(), transport.stdout, transport.exit));
        if let Err(error) = shared.handshake(&ctx.selection).await {
            shared.shutdown_stdin().await;
            return Err(error);
        }
        Ok(OpenedSession {
            session: Arc::new(PiSession { shared }),
            events: receiver,
            capabilities,
        })
    }
}

/// The native session the agent currently serves.
#[derive(Clone, Debug, Default)]
struct Native {
    state: SessionState,
    /// A turn ran on it, or it was resumed with history: a new thread needs
    /// a `new_session`.
    used: bool,
}

struct Shared {
    flavor: PiFlavor,
    normalizer: Mutex<Normalizer>,
    session_id: ProviderSessionId,
    ids: Arc<dyn IdAllocator>,
    clock: Arc<dyn Clock>,
    raw_log: Arc<dyn RawFrameLog>,
    transports: Arc<dyn TransportFactory>,
    instance: InstanceConfig,
    cwd: PathBuf,
    stdin: tokio::sync::Mutex<Option<Box<dyn AsyncWrite + Send + Unpin>>>,
    next_request: AtomicU64,
    pending: Mutex<HashMap<String, oneshot::Sender<Result<Value, String>>>>,
    /// Resolves `start_turn` once the agent took (or refused) the prompt.
    accept: Mutex<Option<oneshot::Sender<Result<(), String>>>>,
    events: Mutex<Option<AdapterEventSender>>,
    native: Mutex<Native>,
    selection: Mutex<Selection>,
    /// The agent exited: the session is lost or closed.
    dead: AtomicBool,
    capabilities: Capabilities,
}

fn protocol_error(detail: impl Into<String>) -> DriverError {
    DriverError::Protocol {
        reason: ProtocolReason::UnexpectedFrame,
        detail: detail.into(),
    }
}

impl Shared {
    fn name(&self) -> &'static str {
        self.flavor.display_name()
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

    async fn write(&self, frame: &Value) -> DriverResult<()> {
        let mut line = serde_json::to_vec(frame).map_err(|error| DriverError::Protocol {
            reason: ProtocolReason::Decode,
            detail: error.to_string(),
        })?;
        line.push(b'\n');
        let mut stdin = self.stdin.lock().await;
        let Some(stdin) = stdin.as_mut() else {
            return Err(DriverError::SessionLost {
                detail: format!("{} is not running", self.name()),
            });
        };
        stdin.write_all(&line).await?;
        stdin.flush().await?;
        Ok(())
    }

    async fn shutdown_stdin(&self) {
        if let Some(mut stdin) = self.stdin.lock().await.take() {
            let _ = stdin.shutdown().await;
        }
    }

    fn request_id(&self) -> String {
        format!("waku-{}", self.next_request.fetch_add(1, Ordering::Relaxed))
    }

    /// Sends a command and waits for its response.
    async fn request(&self, mut frame: Value, timeout: Duration) -> DriverResult<Value> {
        if self.dead.load(Ordering::Acquire) {
            return Err(DriverError::SessionLost {
                detail: format!("the {} session ended", self.name()),
            });
        }
        let id = self.request_id();
        frame["id"] = Value::String(id.clone());
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().insert(id.clone(), sender);
        if let Err(error) = self.write(&frame).await {
            self.pending.lock().remove(&id);
            return Err(error);
        }
        match tokio::time::timeout(timeout, receiver).await {
            Ok(Ok(Ok(response))) => Ok(response),
            Ok(Ok(Err(error))) => Err(protocol_error(error)),
            Ok(Err(_)) => Err(DriverError::SessionLost {
                detail: format!("{} exited before answering", self.name()),
            }),
            Err(_) => {
                self.pending.lock().remove(&id);
                Err(DriverError::Protocol {
                    reason: ProtocolReason::Timeout,
                    detail: format!("{} timed out", frame["type"].as_str().unwrap_or("request")),
                })
            }
        }
    }

    /// `get_state`: the session the agent serves now.
    async fn refresh_state(&self, timeout: Duration) -> DriverResult<SessionState> {
        let response = self.request(json!({"type": "get_state"}), timeout).await?;
        let state = protocol::session_state(&response)
            .ok_or_else(|| protocol_error(format!("{} reported no session id", self.name())))?;
        if let Some(window) = state.context_window {
            self.normalizer.lock().window = Some(window);
        }
        self.native.lock().state = state.clone();
        Ok(state)
    }

    async fn handshake(&self, selection: &Selection) -> DriverResult<()> {
        if self.flavor.negotiates_protocol_v2() {
            // Before anything else, so a large first response arrives chunked
            // instead of shrunk to an error frame.
            self.request(
                json!({"type": "negotiate_protocol", "protocolVersion": 2}),
                INITIALIZE_TIMEOUT,
            )
            .await?;
        }
        self.refresh_state(INITIALIZE_TIMEOUT).await?;
        if let Some(model) = selection.model.as_deref() {
            self.set_model(model).await?;
        }
        if let Some(level) = selection.reasoning_effort.as_deref() {
            self.request(
                json!({"type": "set_thinking_level", "level": level}),
                INITIALIZE_TIMEOUT,
            )
            .await?;
        }
        Ok(())
    }

    async fn set_model(&self, model: &str) -> DriverResult<()> {
        let Some(request) = protocol::set_model(model) else {
            self.raw_log.record(
                "main",
                FrameDirection::Note,
                &format!("model {model} is not provider/model; left to the agent"),
            );
            return Ok(());
        };
        let response = self.request(request, RPC_TIMEOUT).await?;
        if let Some(window) = response
            .pointer("/data/contextWindow")
            .and_then(Value::as_u64)
            .filter(|window| *window > 0)
        {
            self.normalizer.lock().window = Some(window);
        }
        Ok(())
    }

    /// Runs a session change (`new_session`, `switch_session`, a fork or
    /// branch) and reads back the session it led to.
    async fn change_session(&self, request: Value) -> DriverResult<SessionState> {
        let response = self.request(request, RPC_TIMEOUT).await?;
        if response.pointer("/data/cancelled").and_then(Value::as_bool) == Some(true) {
            return Err(protocol_error(format!(
                "a {} extension cancelled the session change",
                self.name()
            )));
        }
        self.refresh_state(RPC_TIMEOUT).await
    }

    fn reference(
        &self,
        provider_thread_id: ProviderThreadId,
        state: &SessionState,
    ) -> ProviderThreadRef {
        ProviderThreadRef {
            provider_thread_id,
            native: NativeRef::strong(&state.session_id),
            cursor: Some(
                self.flavor
                    .cursor(&state.session_id, state.session_file.clone()),
            ),
        }
    }

    /// Puts the agent on `thread`'s native session.
    async fn switch_to(&self, thread: &ProviderThreadRef) -> DriverResult<()> {
        let current = self.native.lock().state.session_id.clone();
        if current != thread.native.id {
            let file = self
                .flavor
                .session_file(thread.cursor.as_ref())
                .ok_or(DriverError::NotReady(NotReadyReason::ThreadMissing))?;
            let state = self
                .change_session(json!({"type": "switch_session", "sessionPath": file}))
                .await?;
            if state.session_id != thread.native.id {
                return Err(protocol_error(format!(
                    "{} switched to another session than the one asked for",
                    self.name()
                )));
            }
        }
        self.native.lock().used = true;
        self.normalizer.lock().thread = Some(thread.clone());
        Ok(())
    }

    /// Applies one normalized frame's effects, in order.
    async fn apply(&self, output: Output) {
        let Output {
            mut events,
            writes,
            accepted,
            response,
            title,
            turn_ended: _,
        } = output;
        if let Some(title) = title
            && let Some(row) = self.title_row(title)
        {
            events.push(row);
        }
        self.emit(events).await;
        for frame in writes {
            if let Err(error) = self.write(&frame).await {
                self.raw_log.record(
                    "main",
                    FrameDirection::Note,
                    &format!("automatic reply failed: {error}"),
                );
            }
        }
        if let Some(accepted) = accepted
            && let Some(sender) = self.accept.lock().take()
        {
            let _ = sender.send(accepted);
        }
        if let Some((id, result)) = response
            && let Some(sender) = self.pending.lock().remove(&id)
        {
            let _ = sender.send(result);
        }
    }

    /// The native title as a partial snapshot of the bound provider thread
    /// (the manager merges it onto the stored row).
    fn title_row(&self, title: Option<String>) -> Option<AdapterEvent> {
        let thread = self.normalizer.lock().thread.clone()?;
        let now = self.clock.now();
        let mut row = ProviderThread {
            id: thread.provider_thread_id,
            driver: self.flavor.driver_kind(),
            provider_instance_id: self.instance.instance_id.clone(),
            provider_session_id: None,
            app_thread_id: None,
            owner_node_id: None,
            native_thread_ref: None,
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
        };
        thread.apply_to(&mut row);
        let metadata = row.native_metadata.get_or_insert_with(Default::default);
        metadata.title = title;
        metadata.updated_at = Some(now);
        Some(AdapterEvent::ProviderThreadUpdated {
            thread: row,
            retained_native_turn_ids: None,
        })
    }
}

/// Pi writes diagnostics on stderr; an unread pipe would eventually block
/// it, so stderr goes to the raw log.
fn drain_stderr(stderr: Box<dyn AsyncRead + Send + Unpin>, raw_log: Arc<dyn RawFrameLog>) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if !line.trim().is_empty() {
                raw_log.record("main", FrameDirection::Note, &line);
            }
        }
    });
}

async fn read_loop(
    shared: Arc<Shared>,
    stdout: Box<dyn AsyncRead + Send + Unpin>,
    exit: futures_util::future::BoxFuture<'static, std::io::Result<crate::driver::v2::ProcessExit>>,
) {
    let mut lines = BufReader::new(stdout).lines();
    let mut chunks = ChunkAssembly::default();
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            shared
                .raw_log
                .record("main", FrameDirection::Inbound, &line);
            continue;
        };
        // A chunked frame arrives as an uninterrupted run of `rpc_chunk`
        // envelopes that reassemble into one logical message.
        let frame = match chunks.accept(value) {
            Ok(Some(frame)) => frame,
            Ok(None) => continue,
            Err(error) => {
                shared.raw_log.record("main", FrameDirection::Note, &error);
                continue;
            }
        };
        let output = shared.normalizer.lock().handle(&frame);
        match output {
            Some(output) => shared.apply(output).await,
            None => shared
                .raw_log
                .record("main", FrameDirection::Inbound, &line),
        }
    }
    let status = exit.await;
    // The agent is gone: closed by `close()` or lost. Every open turn gets
    // its one terminal and the stream ends.
    let detail = match &status {
        Ok(exit) if exit.success => format!("{} exited.", shared.name()),
        Ok(exit) => format!("{} exited ({:?}).", shared.name(), exit.code),
        Err(error) => format!("{} failed: {error}", shared.name()),
    };
    let events = {
        let mut normalizer = shared.normalizer.lock();
        let mut output = Output::default();
        match normalizer.turn.as_ref() {
            Some(turn) if turn.accepted => normalizer.finish_turn(
                TurnOutcome::Failed {
                    failure: ProviderFailure::new(FailureClass::TransportError, detail.clone())
                        .with_code("process_exit"),
                },
                Some(detail),
                &mut output,
            ),
            Some(turn) => {
                // Never accepted: `start_turn` fails with `SessionLost`.
                let attempt = turn.run.attempt_id;
                normalizer.abandon_turn(attempt);
            }
            None => {}
        }
        let mut events = normalizer.flush();
        events.extend(output.events);
        events
    };
    shared.emit(events).await;
    shared.dead.store(true, Ordering::Release);
    shared.accept.lock().take();
    shared.pending.lock().clear();
    shared.events.lock().take();
    shared.stdin.lock().await.take();
}

struct PiSession {
    shared: Arc<Shared>,
}

/// The text Pi receives for a turn: context blocks in the shared envelope
/// (Pi has no system or developer channel), then the message with
/// attachment paths mentioned (native images arrive with #78).
fn turn_text(
    app_context: &[ContextBlock],
    message: &str,
    attachments: &[(String, PathBuf)],
) -> String {
    let mut text = message.to_owned();
    for (name, path) in attachments {
        text.push_str(&format!(
            "\n\n[Attached file: {name} at {}]",
            path.display()
        ));
    }
    wrap_synthetic_context(app_context, &text)
}

impl PiSession {
    /// Copies `source` with a throwaway agent and keeps all but the last
    /// `turns_to_remove` user turns of the copy.
    async fn fork_copy(
        &self,
        source: &ProviderThreadRef,
        turns_to_remove: usize,
    ) -> DriverResult<SessionState> {
        let shared = &self.shared;
        let file = shared
            .flavor
            .session_file(source.cursor.as_ref())
            .or_else(|| {
                let native = shared.native.lock();
                (native.state.session_id == source.native.id)
                    .then(|| native.state.session_file.clone())
                    .flatten()
            })
            .ok_or(DriverError::NotReady(NotReadyReason::ThreadMissing))?;
        let (args, env) = protocol::fork_launch(shared.flavor, &file, &shared.instance);
        let transport = shared
            .transports
            .spawn_stdio(
                "fork-1",
                ProcessSpec {
                    program: shared.instance.binary.clone(),
                    args,
                    env,
                    cwd: shared.cwd.clone(),
                },
            )
            .await?;
        drain_stderr(transport.stderr, shared.raw_log.clone());
        let mut side = SideAgent {
            stdin: Some(transport.stdin),
            lines: BufReader::new(transport.stdout).lines(),
            chunks: ChunkAssembly::default(),
            next: 1,
            raw_log: shared.raw_log.clone(),
        };
        let result = async {
            if shared.flavor.negotiates_protocol_v2() {
                side.request(json!({"type": "negotiate_protocol", "protocolVersion": 2}))
                    .await?;
            }
            if turns_to_remove > 0 {
                let messages = side
                    .request(json!({"type": shared.flavor.branch_messages_command()}))
                    .await?;
                let messages = messages
                    .pointer("/data/messages")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let request = protocol::branch_request(shared.flavor, &messages, turns_to_remove)
                    .map_err(protocol_error)?;
                if let Some(request) = request {
                    let response = side.request(request).await?;
                    if response.pointer("/data/cancelled").and_then(Value::as_bool) == Some(true) {
                        return Err(protocol_error("an extension cancelled the fork"));
                    }
                }
            }
            let state = side.request(json!({"type": "get_state"})).await?;
            protocol::session_state(&state)
                .filter(|state| state.session_id != source.native.id)
                .ok_or_else(|| protocol_error("the fork did not create a distinct session"))
        }
        .await;
        if let Some(mut stdin) = side.stdin.take() {
            let _ = stdin.shutdown().await;
        }
        // Drain what is left so the agent can exit, then reap it.
        let mut lines = side.lines;
        let _ = tokio::time::timeout(FORK_TIMEOUT, async move {
            while let Ok(Some(_)) = lines.next_line().await {}
            let _ = transport.exit.await;
        })
        .await;
        result
    }
}

/// A short-lived agent spoken to over its own channel.
struct SideAgent {
    stdin: Option<Box<dyn AsyncWrite + Send + Unpin>>,
    lines: tokio::io::Lines<BufReader<Box<dyn AsyncRead + Send + Unpin>>>,
    chunks: ChunkAssembly,
    next: u64,
    raw_log: Arc<dyn RawFrameLog>,
}

impl SideAgent {
    async fn request(&mut self, mut frame: Value) -> DriverResult<Value> {
        let id = format!("waku-{}", self.next);
        self.next += 1;
        frame["id"] = Value::String(id.clone());
        let mut line = serde_json::to_vec(&frame).map_err(|error| DriverError::Protocol {
            reason: ProtocolReason::Decode,
            detail: error.to_string(),
        })?;
        line.push(b'\n');
        let stdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| DriverError::SessionLost {
                detail: "the fork agent is closed".to_owned(),
            })?;
        stdin.write_all(&line).await?;
        stdin.flush().await?;
        let read = async {
            while let Ok(Some(line)) = self.lines.next_line().await {
                let Ok(value) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                let Ok(Some(value)) = self.chunks.accept(value) else {
                    continue;
                };
                if value.get("type").and_then(Value::as_str) == Some("response")
                    && value.get("id").and_then(Value::as_str) == Some(id.as_str())
                {
                    return Some(value);
                }
                self.raw_log
                    .record("fork-1", FrameDirection::Inbound, &line);
            }
            None
        };
        let response = tokio::time::timeout(FORK_TIMEOUT, read)
            .await
            .map_err(|_| DriverError::Protocol {
                reason: ProtocolReason::Timeout,
                detail: "the fork agent timed out".to_owned(),
            })?
            .ok_or_else(|| DriverError::SessionLost {
                detail: "the fork agent exited".to_owned(),
            })?;
        if response.get("success").and_then(Value::as_bool) == Some(true) {
            Ok(response)
        } else {
            Err(protocol_error(
                response
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("the fork agent refused")
                    .to_owned(),
            ))
        }
    }
}

#[async_trait]
impl DriverSession for PiSession {
    fn capabilities(&self) -> &Capabilities {
        &self.shared.capabilities
    }

    async fn ensure_thread(&self, req: EnsureThread) -> DriverResult<ProviderThreadRef> {
        let shared = &self.shared;
        let used = shared.native.lock().used;
        let state = if used {
            shared
                .change_session(json!({"type": "new_session"}))
                .await?
        } else {
            shared.native.lock().state.clone()
        };
        let provider_thread_id = match &req.existing {
            Some(existing) => existing.provider_thread_id,
            None => ProviderThreadId(shared.ids.bind(
                shared.session_id.0,
                EntityKind::ProviderThread,
                &state.session_id,
            )),
        };
        let reference = shared.reference(provider_thread_id, &state);
        shared.native.lock().used = false;
        shared.normalizer.lock().thread = Some(reference.clone());
        Ok(reference)
    }

    async fn resume_thread(&self, req: ResumeThread) -> DriverResult<ProviderThreadRef> {
        if !req.thread.is_bound() {
            return Err(DriverError::NotReady(NotReadyReason::ThreadMissing));
        }
        self.shared.switch_to(&req.thread).await?;
        Ok(req.thread)
    }

    async fn start_turn(&self, req: TurnStart) -> DriverResult<TurnAccepted> {
        let shared = &self.shared;
        if shared.dead.load(Ordering::Acquire) {
            return Err(DriverError::SessionLost {
                detail: format!("the {} session ended", shared.name()),
            });
        }
        if shared.normalizer.lock().turn.is_some() {
            return Err(DriverError::NotReady(NotReadyReason::PreviousTurnActive));
        }
        let bound = shared.native.lock().state.session_id == req.thread.native.id;
        if !bound {
            shared.switch_to(&req.thread).await?;
        } else {
            shared.normalizer.lock().thread = Some(req.thread.clone());
        }
        let attachments = req
            .message
            .attachments
            .iter()
            .map(|attachment| (attachment.name.clone(), attachment.path.clone()))
            .collect::<Vec<_>>();
        let text = turn_text(&req.app_context, &req.message.text, &attachments);
        let id = shared.request_id();
        let (sender, accepted) = oneshot::channel();
        *shared.accept.lock() = Some(sender);
        let reference = shared.normalizer.lock().begin_turn(
            req.run,
            &req.thread,
            req.provider_turn_ordinal,
            id.clone(),
        );
        shared.native.lock().used = true;
        if let Err(error) = shared
            .write(&json!({"id": id, "type": "prompt", "message": text}))
            .await
        {
            shared.normalizer.lock().abandon_turn(req.run.attempt_id);
            return Err(error);
        }
        match accepted.await {
            Ok(Ok(())) => Ok(TurnAccepted {
                provider_turn: Some(reference),
            }),
            Ok(Err(message)) => Err(DriverError::Provider(
                ProviderFailure::new(FailureClass::Unknown, message).with_code("prompt_rejected"),
            )),
            Err(_) => {
                shared.normalizer.lock().abandon_turn(req.run.attempt_id);
                Err(DriverError::SessionLost {
                    detail: format!("{} exited before it took the prompt", shared.name()),
                })
            }
        }
    }

    async fn steer_turn(&self, req: Steer) -> DriverResult<()> {
        let active = self
            .shared
            .normalizer
            .lock()
            .turn
            .as_ref()
            .is_some_and(|turn| turn.run.run_id == req.run.run_id);
        if !active {
            return Err(DriverError::SteerRejected {
                reason: SteerRejectReason::NoActiveTurn,
            });
        }
        // Pi queues the message for the running turn's next model call.
        match self
            .shared
            .request(
                json!({"type": "steer", "message": req.message.text}),
                RPC_TIMEOUT,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(DriverError::Protocol {
                reason: ProtocolReason::UnexpectedFrame,
                ..
            }) => Err(DriverError::SteerRejected {
                reason: SteerRejectReason::ProviderRefused,
            }),
            Err(error) => Err(error),
        }
    }

    async fn interrupt_turn(&self, req: Interrupt) -> DriverResult<()> {
        let shared = self.shared.clone();
        let attempt = req.run.attempt_id;
        if !shared.normalizer.lock().mark_interrupt(attempt) {
            // Nothing of this attempt is running; its terminal was sent.
            return Ok(());
        }
        let id = shared.request_id();
        let (sender, answered) = oneshot::channel();
        shared.pending.lock().insert(id.clone(), sender);
        shared.write(&json!({"id": id, "type": "abort"})).await?;
        // The agent settles an aborted run. One that never started has
        // nothing to settle: it ends `Cancelled` once the abort is answered.
        // One that does not settle in time is settled here.
        tokio::spawn(async move {
            let _ = tokio::time::timeout(RPC_TIMEOUT, answered).await;
            if shared.normalizer.lock().idle_turn(attempt) {
                let mut output = Output::default();
                shared
                    .normalizer
                    .lock()
                    .finish_turn(TurnOutcome::Cancelled, None, &mut output);
                shared.apply(output).await;
                return;
            }
            if !shared.normalizer.lock().turn_open(attempt) {
                return;
            }
            tokio::time::sleep(INTERRUPT_GRACE).await;
            if shared.normalizer.lock().turn_open(attempt) {
                let mut output = Output::default();
                shared.normalizer.lock().finish_turn(
                    TurnOutcome::Interrupted,
                    Some(format!("{} did not settle the aborted turn", shared.name())),
                    &mut output,
                );
                shared.apply(output).await;
            }
        });
        Ok(())
    }

    async fn respond(&self, req: RuntimeResponse) -> DriverResult<()> {
        let answered = self
            .shared
            .normalizer
            .lock()
            .respond(&req.native.id, &req.reply);
        let Some((frame, events)) = answered else {
            return Ok(());
        };
        self.shared.write(&frame).await?;
        self.shared.emit(events).await;
        Ok(())
    }

    async fn read_snapshot(&self, thread: &ProviderThreadRef) -> DriverResult<ThreadSnapshot> {
        let shared = &self.shared;
        let file = shared
            .flavor
            .session_file(thread.cursor.as_ref())
            .ok_or(DriverError::NotReady(NotReadyReason::ThreadMissing))?;
        let provider = match shared.flavor {
            PiFlavor::Pi => waku_protocol::model::ProviderKind::Pi,
            PiFlavor::OhMyPi => waku_protocol::model::ProviderKind::OhMyPi,
        };
        let session = thread.native.id.clone();
        let history = tokio::task::spawn_blocking(move || {
            crate::pi_session::provider_session_history(provider, &session, &file, usize::MAX)
        })
        .await
        .map_err(|error| DriverError::Io(std::io::Error::other(error)))?
        .map_err(|error| protocol_error(error.to_string()))?;
        Ok(crate::driver::history::snapshot_from_history(
            shared.ids.as_ref(),
            shared.clock.now(),
            shared.flavor.driver_kind(),
            &shared.instance,
            thread,
            &history,
        ))
    }

    async fn rollback(&self, req: Rollback) -> DriverResult<RollbackOutcome> {
        let shared = &self.shared;
        if shared.normalizer.lock().turn.is_some() {
            return Err(DriverError::NotReady(NotReadyReason::PreviousTurnActive));
        }
        let turns_to_remove = req.turns_to_remove as usize;
        if turns_to_remove == 0 {
            return Ok(RollbackOutcome {
                thread: req.thread,
                replaced: false,
                snapshot: None,
                retained_native_turn_ids: None,
            });
        }
        shared.switch_to(&req.thread).await?;
        let state = match req.target {
            // Nothing is kept: a fresh session.
            RollbackTarget::ThreadStart => {
                shared
                    .change_session(json!({"type": "new_session"}))
                    .await?
            }
            RollbackTarget::AfterProviderTurn(_) => {
                let messages = shared
                    .request(
                        json!({"type": shared.flavor.branch_messages_command()}),
                        RPC_TIMEOUT,
                    )
                    .await?;
                let messages = messages
                    .pointer("/data/messages")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let request = protocol::branch_request(shared.flavor, &messages, turns_to_remove)
                    .map_err(protocol_error)?
                    .ok_or_else(|| protocol_error("nothing to roll back"))?;
                shared.change_session(request).await?
            }
        };
        let provider_thread_id = ProviderThreadId(shared.ids.bind(
            req.thread.provider_thread_id.0,
            EntityKind::ProviderThread,
            &state.session_id,
        ));
        let thread = shared.reference(provider_thread_id, &state);
        shared.native.lock().used = true;
        shared.normalizer.lock().thread = Some(thread.clone());
        Ok(RollbackOutcome {
            thread,
            replaced: true,
            snapshot: None,
            retained_native_turn_ids: None,
        })
    }

    async fn fork(&self, req: ForkRequest) -> DriverResult<ProviderThreadRef> {
        if self.shared.normalizer.lock().turn.is_some() {
            return Err(DriverError::NotReady(NotReadyReason::PreviousTurnActive));
        }
        let turns_to_remove = match &req.at {
            ForkPoint::Tip => 0,
            ForkPoint::AfterProviderTurn {
                turns_to_remove, ..
            } => *turns_to_remove as usize,
            ForkPoint::SubagentThread(_) => {
                return Err(DriverError::Unsupported {
                    operation: "fork_from_subagent_thread",
                });
            }
        };
        let state = self.fork_copy(&req.source, turns_to_remove).await?;
        let provider_thread_id = ProviderThreadId(self.shared.ids.bind(
            req.source.provider_thread_id.0,
            EntityKind::ProviderThread,
            &state.session_id,
        ));
        Ok(self.shared.reference(provider_thread_id, &state))
    }

    async fn apply_selection(&self, to: &Selection) -> DriverResult<()> {
        let from = self.shared.selection.lock().clone();
        if from.model != to.model
            && let Some(model) = to.model.as_deref()
        {
            self.shared.set_model(model).await?;
        }
        if from.reasoning_effort != to.reasoning_effort
            && let Some(level) = to.reasoning_effort.as_deref()
        {
            self.shared
                .request(
                    json!({"type": "set_thinking_level", "level": level}),
                    RPC_TIMEOUT,
                )
                .await?;
        }
        *self.shared.selection.lock() = to.clone();
        Ok(())
    }

    async fn close(&self) -> DriverResult<()> {
        // Closing stdin is the shutdown signal; the read loop ends the
        // stream once the agent exits.
        self.shared.shutdown_stdin().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests;

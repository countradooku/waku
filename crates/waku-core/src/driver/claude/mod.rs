//! Claude Code on `DriverV2` (RFC 0002 "Claude", #47).
//!
//! `claude -p` in streaming-input mode serves one conversation per process:
//! user messages go in on stdin as stream-json, `control_request`s carry
//! interrupts, model switches and task stops, and `--permission-prompt-tool
//! stdio` turns every permission check into a `can_use_tool` request Waku
//! answers. That is the same protocol the Agent SDK's `query()` speaks, so
//! the driver speaks it directly (D-3).
//!
//! One actor per session: `ensure_thread` / `resume_thread` only choose the
//! conversation, and the process starts with the first turn (`--session-id`
//! for a new conversation, `--resume` for one with history), so blocks of
//! `ContextKind::System` at that turn ride `--append-system-prompt`. A turn
//! starts at the echoed prompt (`--replay-user-messages`) and ends at
//! `result`. A rollback or fork copies the native transcript up to the kept
//! turn's assistant message (`claude_session`) and replaces the provider
//! thread; the next turn respawns the process on the copy.
//!
//! Processes come only from `ctx.transports` (channel `main`), and native
//! transcripts are read under `transports.provider_home`, so replay covers
//! the whole driver.

mod normalize;
pub mod protocol;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::oneshot;
use waku_protocol::model::ProviderResumeCursor;
use waku_protocol::orchestration::{self as orch, *};

use self::normalize::{Normalizer, Output, Scope};
use self::protocol::Launch;
pub use self::protocol::{CLAUDE_PROTOCOL, CLAUDE_PROVIDER};
use crate::driver::v2::{
    AdapterEvent, AdapterEventSender, BackgroundWorkRef, Clock, ContextKind, DriverError,
    DriverFamily, DriverResult, DriverSession, DriverV2, EnsureThread, EntityKind, ForkPoint,
    ForkRequest, FrameDirection, IdAllocator, InstanceConfig, Interrupt, NativeRef, NotReadyReason,
    OpenedSession, ProcessSpec, ProtocolReason, ProviderThreadRef, ProviderTurnRef, RawFrameLog,
    ResumeThread, Rollback, RollbackOutcome, RollbackTarget, RuntimeResponse, Selection,
    SessionContext, Steer, SteerRejectReason, SubagentRef, SubagentStopOutcome, ThreadSnapshot,
    TransitionPlan, TransportFactory, TurnAccepted, TurnOutcome, TurnStart, adapter_channel,
    wrap_synthetic_context,
};

/// How long a control request (`set_model`, `stop_task`) may take.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);
/// After an interrupt, how long the CLI has to settle the turn with a
/// `result` before the driver retires the process and settles it itself.
const INTERRUPT_GRACE: Duration = Duration::from_secs(10);

/// The capabilities this driver really has (RFC 0002 matrix, Claude
/// column, as amended by #47). Every value is the same for every instance:
/// nothing depends on the CLI version or the handshake yet.
pub fn claude_capabilities() -> Capabilities {
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
    caps.turns.terminal_status_quality = Quality::Strong;
    caps.streaming.assistant_text = true;
    caps.streaming.reasoning = true;
    caps.streaming.emits_message_completed = true;
    caps.tools.tool_item_ids = true;
    caps.tools.emits_tool_started = true;
    caps.tools.emits_tool_completed = true;
    caps.tools.emits_tool_output = true;
    caps.tools.mcp_tools = true;
    caps.approvals.command = true;
    caps.approvals.file_read = true;
    caps.approvals.file_change = true;
    caps.approvals.native_request_ids = true;
    caps.approvals.callbacks_are_live_only = true;
    caps.planning.emits_plan_updated = true;
    caps.planning.emits_todo_list = true;
    caps.planning.emits_proposed_plan = true;
    caps.planning.structured_questions = true;
    caps.subagents.supported = true;
    caps.subagents.emits_lifecycle = true;
    caps.subagents.can_close = true;
    caps.context.accepts_system_context = true;
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
    caps.extras.background_work = true;
    caps.extras.model_discovery = true;
    caps
}

/// An instance of the `claude` CLI found on the search path.
pub fn claude_instance() -> InstanceConfig {
    let mut cfg = InstanceConfig::new(CLAUDE_PROVIDER, orch::DriverKind::Claude);
    cfg.binary = "claude".into();
    cfg
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ClaudeDriver;

#[async_trait]
impl DriverV2 for ClaudeDriver {
    fn kind(&self) -> DriverFamily {
        DriverFamily::Claude
    }

    fn capabilities(&self, _cfg: &InstanceConfig) -> Capabilities {
        claude_capabilities()
    }

    fn plan_selection_transition(
        &self,
        _session_caps: &Capabilities,
        from: &Selection,
        to: &Selection,
    ) -> TransitionPlan {
        let mut plans = Vec::new();
        // The window rides on the model id, so both are a `set_model`.
        if from.model != to.model || from.context_window != to.context_window {
            plans.push(TransitionPlan::ApplyInSession);
        }
        if from.reasoning_effort != to.reasoning_effort {
            plans.push(TransitionPlan::RestartSession);
        }
        if from.service_tier != to.service_tier || from.agent_preset != to.agent_preset {
            plans.push(TransitionPlan::ApplyOnNextTurn);
        }
        // `--permission-mode` is a launch flag; `set_permission_mode` is not
        // proven by a recording yet (RFC 0002 amendments, #47).
        if from.runtime_mode != to.runtime_mode {
            plans.push(TransitionPlan::RestartSession);
        }
        if from.interaction_mode != to.interaction_mode {
            plans.push(TransitionPlan::Reject {
                reason: "Claude Code's plan mode is not wired yet (#68)".to_owned(),
            });
        }
        TransitionPlan::combine(plans)
    }

    async fn open_session(&self, ctx: SessionContext) -> DriverResult<OpenedSession> {
        let capabilities = self.capabilities(&ctx.instance);
        let (events, receiver) = adapter_channel();
        let auto_approve = ctx.selection.runtime_mode != RuntimeMode::Ask;
        let shared = Arc::new(Shared {
            normalizer: Mutex::new(Normalizer::new(
                Scope {
                    session_id: ctx.provider_session_id,
                    thread_id: ctx.thread_id,
                    instance: ctx.instance.instance_id.clone(),
                    ids: ctx.ids.clone(),
                    clock: ctx.clock.clone(),
                },
                auto_approve,
            )),
            session_id: ctx.provider_session_id,
            ids: ctx.ids.clone(),
            clock: ctx.clock.clone(),
            raw_log: ctx.raw_log.clone(),
            transports: ctx.transports.clone(),
            config_dir: config_dir(&ctx.instance, ctx.transports.as_ref()),
            instance: ctx.instance,
            cwd: ctx.cwd,
            process: tokio::sync::Mutex::new(None),
            generation: AtomicU64::new(0),
            retiring: Mutex::new(HashSet::new()),
            next_request: AtomicU64::new(1),
            controls: Mutex::new(HashMap::new()),
            echo: Mutex::new(None),
            events: Mutex::new(Some(events)),
            selection: Mutex::new(ctx.selection),
            used: Mutex::new(HashSet::new()),
            dead: AtomicBool::new(false),
            capabilities: capabilities.clone(),
        });
        Ok(OpenedSession {
            session: Arc::new(ClaudeSession { shared }),
            events: receiver,
            capabilities,
        })
    }
}

/// Claude's configuration directory: the instance's provider home
/// (`CLAUDE_CONFIG_DIR`), else `~/.claude` under the transports' home.
fn config_dir(instance: &InstanceConfig, transports: &dyn TransportFactory) -> PathBuf {
    instance
        .provider_home
        .clone()
        .or_else(|| {
            instance
                .env
                .iter()
                .find(|(key, value)| key == "CLAUDE_CONFIG_DIR" && !value.is_empty())
                .map(|(_, value)| PathBuf::from(value))
        })
        .unwrap_or_else(|| transports.provider_home(instance).join(".claude"))
}

struct Process {
    stdin: Option<Box<dyn AsyncWrite + Send + Unpin>>,
    session_id: String,
    generation: u64,
    launched: Selection,
    exited: Option<oneshot::Receiver<()>>,
}

struct Shared {
    normalizer: Mutex<Normalizer>,
    session_id: ProviderSessionId,
    ids: Arc<dyn IdAllocator>,
    clock: Arc<dyn Clock>,
    raw_log: Arc<dyn RawFrameLog>,
    transports: Arc<dyn TransportFactory>,
    config_dir: PathBuf,
    instance: InstanceConfig,
    cwd: PathBuf,
    process: tokio::sync::Mutex<Option<Process>>,
    generation: AtomicU64,
    /// Generations closed on purpose (a respawn), whose exit is not a loss.
    retiring: Mutex<HashSet<u64>>,
    next_request: AtomicU64,
    controls: Mutex<HashMap<String, oneshot::Sender<Result<Value, String>>>>,
    /// Resolves `start_turn` at the echoed prompt.
    echo: Mutex<Option<oneshot::Sender<()>>>,
    events: Mutex<Option<AdapterEventSender>>,
    /// The selection the next process launches with.
    selection: Mutex<Selection>,
    /// Native conversations that have history (resume, never `--session-id`).
    used: Mutex<HashSet<String>>,
    /// The stream ended: the session is lost or closed.
    dead: AtomicBool,
    capabilities: Capabilities,
}

impl Shared {
    fn projects_dir(&self) -> PathBuf {
        self.config_dir.join("projects")
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
        let mut process = self.process.lock().await;
        let Some(stdin) = process.as_mut().and_then(|process| process.stdin.as_mut()) else {
            return Err(DriverError::SessionLost {
                detail: "Claude Code is not running".to_owned(),
            });
        };
        stdin.write_all(&line).await?;
        stdin.flush().await?;
        Ok(())
    }

    fn request_id(&self) -> String {
        format!("waku-{}", self.next_request.fetch_add(1, Ordering::Relaxed))
    }

    /// Sends a control request and waits for its response.
    async fn control(&self, request: Value) -> DriverResult<Value> {
        let request_id = self.request_id();
        let (sender, receiver) = oneshot::channel();
        self.controls.lock().insert(request_id.clone(), sender);
        if let Err(error) = self
            .write(&protocol::control_request(&request_id, request))
            .await
        {
            self.controls.lock().remove(&request_id);
            return Err(error);
        }
        match tokio::time::timeout(CONTROL_TIMEOUT, receiver).await {
            Ok(Ok(Ok(response))) => Ok(response),
            Ok(Ok(Err(error))) => Err(DriverError::Protocol {
                reason: ProtocolReason::UnexpectedFrame,
                detail: error,
            }),
            Ok(Err(_)) => Err(DriverError::SessionLost {
                detail: "Claude Code exited before answering".to_owned(),
            }),
            Err(_) => {
                self.controls.lock().remove(&request_id);
                Err(DriverError::Protocol {
                    reason: ProtocolReason::Timeout,
                    detail: "control request timed out".to_owned(),
                })
            }
        }
    }

    /// Starts the process for `thread` unless it already serves it. A
    /// process serving another conversation (after a rollback or fork) is
    /// retired first. Returns whether a process was spawned.
    async fn ensure_process(
        self: &Arc<Self>,
        thread: &ProviderThreadRef,
        selection: &Selection,
        system_prompt: Option<&str>,
    ) -> DriverResult<bool> {
        if self.dead.load(Ordering::Acquire) {
            return Err(DriverError::SessionLost {
                detail: "the Claude Code session ended".to_owned(),
            });
        }
        let mut process = self.process.lock().await;
        if let Some(running) = process.as_ref()
            && running.session_id == thread.native.id
        {
            return Ok(false);
        }
        if let Some(mut old) = process.take() {
            self.retiring.lock().insert(old.generation);
            if let Some(mut stdin) = old.stdin.take() {
                let _ = stdin.shutdown().await;
            }
            if let Some(exited) = old.exited.take() {
                let _ = exited.await;
            }
        }
        let session_id = thread.native.id.clone();
        let launch = if self.used.lock().insert(session_id.clone()) {
            Launch::New(session_id.clone())
        } else {
            Launch::Resume(session_id.clone())
        };
        let mut env = self.instance.env.clone();
        if let Some(home) = &self.instance.provider_home
            && !env.iter().any(|(key, _)| key == "CLAUDE_CONFIG_DIR")
        {
            env.push(("CLAUDE_CONFIG_DIR".to_owned(), home.display().to_string()));
        }
        let spec = ProcessSpec {
            program: self.instance.binary.clone(),
            args: protocol::launch_args(selection, &launch, system_prompt),
            env,
            cwd: self.cwd.clone(),
        };
        let transport = self.transports.spawn_stdio("main", spec).await?;
        let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        let (exited_sender, exited) = oneshot::channel();
        {
            let mut normalizer = self.normalizer.lock();
            normalizer.reset_thread(thread.clone());
            normalizer.set_auto_approve(selection.runtime_mode != RuntimeMode::Ask);
        }
        *process = Some(Process {
            stdin: Some(transport.stdin),
            session_id,
            generation,
            launched: selection.clone(),
            exited: Some(exited),
        });
        drop(process);
        *self.selection.lock() = selection.clone();
        // The conversation is addressable from here on (today's `Connected`).
        let resume_at = match &thread.cursor {
            Some(ProviderResumeCursor::Claude { resume_at, .. }) => resume_at.clone(),
            _ => None,
        };
        let cursor = protocol::cursor(&thread.native.id, resume_at.as_deref());
        let row = self.thread_row(thread.clone(), cursor, resume_at.as_deref(), None);
        self.emit(vec![row]).await;
        tokio::spawn(read_loop(
            self.clone(),
            generation,
            transport.stdout,
            transport.exit,
            exited_sender,
        ));
        Ok(true)
    }

    /// Applies one normalized frame's effects, in order.
    async fn apply(self: &Arc<Self>, output: Output) {
        let Output {
            mut events,
            writes,
            echoed,
            control,
            turn_ended,
        } = output;
        if turn_ended {
            self.thread_update(&mut events).await;
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
        if (echoed || turn_ended)
            && let Some(echo) = self.echo.lock().take()
        {
            let _ = echo.send(());
        }
        if let Some((request_id, result)) = control
            && let Some(sender) = self.controls.lock().remove(&request_id)
        {
            let _ = sender.send(result);
        }
    }

    /// Puts the thread's new resume point and native title ahead of the
    /// turn's terminal, as a partial provider-thread snapshot (the manager
    /// merges it onto the stored row; the bridge reports the cursor).
    async fn thread_update(&self, events: &mut Vec<AdapterEvent>) {
        let (thread, resume_at) = {
            let normalizer = self.normalizer.lock();
            (
                normalizer.thread.clone(),
                normalizer.last_assistant_uuid.clone(),
            )
        };
        let Some(thread) = thread else { return };
        let projects = self.projects_dir();
        let lookup = thread.native.id.clone();
        let title = tokio::task::spawn_blocking(move || {
            crate::claude_session::session_metadata_in(&projects, &lookup)
        })
        .await
        .ok()
        .and_then(Result::ok)
        .and_then(|metadata| metadata.title);
        let cursor = protocol::cursor(&thread.native.id, resume_at.as_deref());
        if thread.cursor.as_ref() == Some(&cursor) && title.is_none() {
            return;
        }
        let row = self.thread_row(thread, cursor, resume_at.as_deref(), title);
        let position = events
            .iter()
            .rposition(|event| matches!(event, AdapterEvent::TurnTerminal { .. }))
            .unwrap_or(events.len());
        events.insert(position, row);
    }

    /// A partial provider-thread snapshot carrying the cursor (and the
    /// native title): the manager merges it onto the stored row, the bridge
    /// reports the cursor as `Connected`.
    fn thread_row(
        &self,
        thread: ProviderThreadRef,
        cursor: ProviderResumeCursor,
        resume_at: Option<&str>,
        title: Option<String>,
    ) -> AdapterEvent {
        let now = self.clock.now();
        let mut row = ProviderThread {
            id: thread.provider_thread_id,
            driver: orch::DriverKind::Claude,
            provider_instance_id: self.instance.instance_id.clone(),
            provider_session_id: None,
            app_thread_id: None,
            owner_node_id: None,
            native_thread_ref: None,
            native_conversation_head_ref: resume_at
                .map(|uuid| NativeRef::weak(uuid).to_provider_ref(orch::DriverKind::Claude)),
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
        let reference = ProviderThreadRef {
            cursor: Some(cursor),
            ..thread
        };
        reference.apply_to(&mut row);
        if let Some(metadata) = row.native_metadata.as_mut() {
            metadata.title = title;
            metadata.updated_at = Some(now);
        }
        self.normalizer.lock().thread = Some(reference);
        AdapterEvent::ProviderThreadUpdated {
            thread: row,
            retained_native_turn_ids: None,
        }
    }
}

async fn read_loop(
    shared: Arc<Shared>,
    generation: u64,
    stdout: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
    exit: futures_util::future::BoxFuture<'static, std::io::Result<crate::driver::v2::ProcessExit>>,
    exited: oneshot::Sender<()>,
) {
    let mut lines = BufReader::new(stdout).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        if shared.retiring.lock().contains(&generation) {
            // A retired process's late frames must not reach the next turn.
            continue;
        }
        let Ok(frame) = serde_json::from_str::<Value>(&line) else {
            shared
                .raw_log
                .record("main", FrameDirection::Inbound, &line);
            continue;
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
    let retired = shared.retiring.lock().remove(&generation);
    if !retired {
        // The conversation's process is gone: closed by `close()` or lost.
        // Every open turn gets its one terminal and the stream ends.
        let detail = match &status {
            Ok(exit) if exit.success => "Claude Code exited.".to_owned(),
            Ok(exit) => format!("Claude Code exited ({:?}).", exit.code),
            Err(error) => format!("Claude Code failed: {error}"),
        };
        let events = {
            let mut normalizer = shared.normalizer.lock();
            let mut output = Output::default();
            let echoed = normalizer.turn.as_ref().is_some_and(|turn| turn.echoed);
            if echoed {
                normalizer.finish_turn(
                    TurnOutcome::Failed {
                        failure: ProviderFailure::new(FailureClass::TransportError, detail.clone())
                            .with_code("process_exit"),
                    },
                    Some(detail),
                    &mut output,
                );
            } else if let Some(turn) = normalizer.turn.as_ref() {
                // Never accepted: `start_turn` fails with `SessionLost`.
                let attempt = turn.run.attempt_id;
                normalizer.abandon_turn(attempt);
            }
            let mut events = normalizer.flush();
            events.extend(output.events);
            events
        };
        shared.emit(events).await;
        shared.dead.store(true, Ordering::Release);
        shared.echo.lock().take();
        shared.controls.lock().clear();
        shared.events.lock().take();
        let mut process = shared.process.lock().await;
        if process
            .as_ref()
            .is_some_and(|process| process.generation == generation)
        {
            process.take();
        }
    }
    let _ = exited.send(());
}

struct ClaudeSession {
    shared: Arc<Shared>,
}

/// The text Claude receives for a turn: synthetic context in the shared
/// envelope, then the message with attachment paths mentioned (native image
/// input arrives with #78).
fn turn_text(req: &TurnStart, system_in_launch: bool) -> String {
    let blocks = req
        .app_context
        .iter()
        .filter(|block| !(system_in_launch && block.kind == ContextKind::System))
        .cloned()
        .collect::<Vec<_>>();
    let mut text = req.message.text.clone();
    for attachment in &req.message.attachments {
        text.push_str(&format!(
            "\n\n[Attached file: {} at {}]",
            attachment.name,
            attachment.path.display()
        ));
    }
    wrap_synthetic_context(&blocks, &text)
}

fn system_prompt(req: &TurnStart) -> Option<String> {
    let blocks = req
        .app_context
        .iter()
        .filter(|block| block.kind == ContextKind::System)
        .map(|block| block.text.clone())
        .collect::<Vec<_>>();
    (!blocks.is_empty()).then(|| blocks.join("\n\n"))
}

fn native_session(thread: &ProviderThreadRef) -> String {
    match &thread.cursor {
        Some(ProviderResumeCursor::Claude { session_id, .. }) if !session_id.is_empty() => {
            session_id.clone()
        }
        _ => thread.native.id.clone(),
    }
}

impl ClaudeSession {
    /// Copies the native transcript up to `up_to` into a new conversation
    /// and returns it as a provider thread replacing (or forked from)
    /// `source`. `None` starts an empty conversation (a rollback to the
    /// thread's start).
    async fn copy_thread(
        &self,
        source: &ProviderThreadRef,
        up_to: Option<String>,
        title: &str,
    ) -> DriverResult<ProviderThreadRef> {
        let shared = &self.shared;
        let new_session = shared.ids.fresh(EntityKind::ProviderThread).to_string();
        let provider_thread_id: ProviderThreadId = ProviderThreadId(shared.ids.bind(
            source.provider_thread_id.0,
            EntityKind::ProviderThread,
            &new_session,
        ));
        let Some(up_to) = up_to else {
            return Ok(ProviderThreadRef {
                provider_thread_id,
                native: NativeRef::strong(&new_session),
                cursor: Some(protocol::cursor(&new_session, None)),
            });
        };
        let projects = shared.projects_dir();
        let source_session = native_session(source);
        let timestamp = shared.clock.now();
        let title = title.to_owned();
        let fork_session = new_session.clone();
        let forked = tokio::task::spawn_blocking(move || {
            crate::claude_session::fork_session_with(
                &projects,
                &source_session,
                &up_to,
                &title,
                crate::claude_session::ForkIds::derived(fork_session, timestamp),
            )
            .map(|fork| {
                let resume_at = fork.message_ids.get(&up_to).cloned();
                (fork.session_id, resume_at)
            })
        })
        .await
        .map_err(|error| DriverError::Io(std::io::Error::other(error)))?
        .map_err(|error| DriverError::Protocol {
            reason: ProtocolReason::UnexpectedFrame,
            detail: error.to_string(),
        })?;
        let (session_id, resume_at) = forked;
        shared.used.lock().insert(session_id.clone());
        Ok(ProviderThreadRef {
            provider_thread_id,
            native: NativeRef::strong(&session_id),
            cursor: Some(protocol::cursor(&session_id, resume_at.as_deref())),
        })
    }

    /// The assistant uuid a kept turn ends at: its weak native id, else the
    /// transcript's checkpoint for that turn ordinal.
    async fn turn_end(
        &self,
        thread: &ProviderThreadRef,
        turn: &ProviderTurnRef,
    ) -> DriverResult<String> {
        if !turn.native.id.is_empty() {
            return Ok(turn.native.id.clone());
        }
        let projects = self.shared.projects_dir();
        let session = native_session(thread);
        let ordinal = turn.ordinal as usize;
        tokio::task::spawn_blocking(move || {
            crate::claude_session::message_id_for_turn_in(&projects, &session, ordinal)
        })
        .await
        .map_err(|error| DriverError::Io(std::io::Error::other(error)))?
        .map_err(|error| DriverError::Protocol {
            reason: ProtocolReason::UnexpectedFrame,
            detail: error.to_string(),
        })
    }

    async fn stop_task(&self, task_id: &str) -> DriverResult<bool> {
        let response = self
            .shared
            .control(json!({"subtype": "stop_task", "task_id": task_id}))
            .await;
        match response {
            Ok(response) => {
                let status = response
                    .get("status")
                    .and_then(Value::as_str)
                    .or_else(|| response.as_str());
                Ok(!matches!(status, Some("not_found" | "not_running")))
            }
            Err(DriverError::Protocol {
                reason: ProtocolReason::UnexpectedFrame,
                ..
            }) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

#[async_trait]
impl DriverSession for ClaudeSession {
    fn capabilities(&self) -> &Capabilities {
        &self.shared.capabilities
    }

    async fn ensure_thread(&self, req: EnsureThread) -> DriverResult<ProviderThreadRef> {
        // Claude takes a caller-chosen session id, so the conversation has
        // its id before it has a message. No I/O: the process starts with
        // the first turn.
        let session_id = self
            .shared
            .ids
            .fresh(EntityKind::ProviderThread)
            .to_string();
        let provider_thread_id = match &req.existing {
            Some(existing) => existing.provider_thread_id,
            None => ProviderThreadId(self.shared.ids.bind(
                self.shared.session_id.0,
                EntityKind::ProviderThread,
                &session_id,
            )),
        };
        Ok(ProviderThreadRef {
            provider_thread_id,
            native: NativeRef::strong(&session_id),
            cursor: Some(protocol::cursor(&session_id, None)),
        })
    }

    async fn resume_thread(&self, req: ResumeThread) -> DriverResult<ProviderThreadRef> {
        let session_id = native_session(&req.thread);
        if session_id.is_empty() {
            return Err(DriverError::NotReady(NotReadyReason::ThreadMissing));
        }
        self.shared.used.lock().insert(session_id);
        Ok(req.thread)
    }

    async fn start_turn(&self, req: TurnStart) -> DriverResult<TurnAccepted> {
        if self.shared.normalizer.lock().turn.is_some() {
            return Err(DriverError::NotReady(NotReadyReason::PreviousTurnActive));
        }
        let system = system_prompt(&req);
        let mut thread = req.thread.clone();
        thread.native.id = native_session(&thread);
        let spawned = self
            .shared
            .ensure_process(&thread, &req.selection, system.as_deref())
            .await?;
        let text = turn_text(&req, spawned);
        let (echo, echoed) = oneshot::channel();
        *self.shared.echo.lock() = Some(echo);
        let reference =
            self.shared
                .normalizer
                .lock()
                .begin_turn(req.run, &thread, req.provider_turn_ordinal);
        if let Err(error) = self.shared.write(&protocol::user_message(&text)).await {
            self.shared
                .normalizer
                .lock()
                .abandon_turn(req.run.attempt_id);
            return Err(error);
        }
        if echoed.await.is_err() {
            self.shared
                .normalizer
                .lock()
                .abandon_turn(req.run.attempt_id);
            return Err(DriverError::SessionLost {
                detail: "Claude Code exited before it took the prompt".to_owned(),
            });
        }
        Ok(TurnAccepted {
            provider_turn: Some(reference),
        })
    }

    async fn steer_turn(&self, req: Steer) -> DriverResult<()> {
        // A user message written mid-turn is folded into the running turn at
        // the next model call (verified against CLI 2.1.223). It does not
        // cancel running tools: no `priority`, so the turn goes on.
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
        self.shared
            .write(&protocol::user_message(&req.message.text))
            .await
    }

    async fn interrupt_turn(&self, req: Interrupt) -> DriverResult<()> {
        if !self
            .shared
            .normalizer
            .lock()
            .mark_interrupt(req.run.attempt_id)
        {
            // Nothing of this attempt is running; its terminal was sent.
            return Ok(());
        }
        let request_id = self.shared.request_id();
        self.shared
            .write(&protocol::control_request(
                &request_id,
                json!({"subtype": "interrupt"}),
            ))
            .await?;
        // The CLI settles an interrupted turn with a `result`. If it does
        // not, retire the process and settle the turn here; the next turn
        // resumes the conversation in a new process.
        let shared = self.shared.clone();
        let attempt = req.run.attempt_id;
        tokio::spawn(async move {
            tokio::time::sleep(INTERRUPT_GRACE).await;
            let still_open = shared
                .normalizer
                .lock()
                .turn
                .as_ref()
                .is_some_and(|turn| turn.run.attempt_id == attempt);
            if !still_open {
                return;
            }
            let mut process = shared.process.lock().await;
            if let Some(mut old) = process.take() {
                shared.retiring.lock().insert(old.generation);
                if let Some(mut stdin) = old.stdin.take() {
                    let _ = stdin.shutdown().await;
                }
            }
            drop(process);
            let mut output = Output::default();
            shared.normalizer.lock().finish_turn(
                TurnOutcome::Interrupted,
                Some("Claude Code did not settle the interrupted turn".to_owned()),
                &mut output,
            );
            shared.apply(output).await;
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
        let projects = self.shared.projects_dir();
        let session = native_session(thread);
        let history = tokio::task::spawn_blocking(move || {
            crate::claude_session::provider_session_history_in(&projects, &session, usize::MAX)
        })
        .await
        .map_err(|error| DriverError::Io(std::io::Error::other(error)))?
        .map_err(|error| DriverError::Protocol {
            reason: ProtocolReason::UnexpectedFrame,
            detail: error.to_string(),
        })?;
        Ok(snapshot_from_history(&self.shared, thread, &history))
    }

    async fn rollback(&self, req: Rollback) -> DriverResult<RollbackOutcome> {
        if self.shared.normalizer.lock().turn.is_some() {
            return Err(DriverError::NotReady(NotReadyReason::PreviousTurnActive));
        }
        let up_to = match &req.target {
            RollbackTarget::ThreadStart => None,
            RollbackTarget::AfterProviderTurn(turn) => {
                Some(self.turn_end(&req.thread, turn).await?)
            }
        };
        let thread = self.copy_thread(&req.thread, up_to, "Waku rewind").await?;
        Ok(RollbackOutcome {
            thread,
            replaced: true,
            snapshot: None,
            retained_native_turn_ids: None,
        })
    }

    async fn fork(&self, req: ForkRequest) -> DriverResult<ProviderThreadRef> {
        let up_to = match &req.at {
            ForkPoint::AfterProviderTurn { turn, .. } => self.turn_end(&req.source, turn).await?,
            ForkPoint::Tip => {
                let projects = self.shared.projects_dir();
                let session = native_session(&req.source);
                tokio::task::spawn_blocking(move || {
                    crate::claude_session::session_metadata_in(&projects, &session)
                })
                .await
                .map_err(|error| DriverError::Io(std::io::Error::other(error)))?
                .ok()
                .and_then(|metadata| metadata.latest_message_id)
                .ok_or_else(|| DriverError::Protocol {
                    reason: ProtocolReason::UnexpectedFrame,
                    detail: "the Claude conversation has no message to fork at".to_owned(),
                })?
            }
            ForkPoint::SubagentThread(_) => {
                return Err(DriverError::Unsupported {
                    operation: "fork_from_subagent_thread",
                });
            }
        };
        self.copy_thread(&req.source, Some(up_to), "Waku fork")
            .await
    }

    async fn apply_selection(&self, to: &Selection) -> DriverResult<()> {
        let launched = {
            let process = self.shared.process.lock().await;
            process.as_ref().map(|process| process.launched.clone())
        };
        let Some(launched) = launched else {
            *self.shared.selection.lock() = to.clone();
            return Ok(());
        };
        let current = protocol::wire_model(
            launched.model.as_deref(),
            launched.context_window.as_deref(),
        );
        let next = protocol::wire_model(to.model.as_deref(), to.context_window.as_deref());
        if current != next {
            self.shared
                .control(json!({"subtype": "set_model", "model": next}))
                .await?;
        }
        if let Some(process) = self.shared.process.lock().await.as_mut() {
            process.launched.model = to.model.clone();
            process.launched.context_window = to.context_window.clone();
        }
        *self.shared.selection.lock() = to.clone();
        Ok(())
    }

    async fn stop_background_work(&self, work: BackgroundWorkRef) -> DriverResult<()> {
        if self.stop_task(&work.task_id).await? {
            Ok(())
        } else {
            Err(DriverError::Protocol {
                reason: ProtocolReason::UnexpectedFrame,
                detail: "the task is no longer running".to_owned(),
            })
        }
    }

    async fn stop_subagent(&self, subagent: SubagentRef) -> DriverResult<SubagentStopOutcome> {
        let task_id = if subagent.native_task_id.is_empty() {
            self.shared
                .normalizer
                .lock()
                .subagent_task(subagent.node_id)
        } else {
            Some(subagent.native_task_id.clone())
        };
        let Some(task_id) = task_id else {
            return Ok(SubagentStopOutcome::AlreadyFinished);
        };
        Ok(if self.stop_task(&task_id).await? {
            SubagentStopOutcome::Stopped
        } else {
            SubagentStopOutcome::AlreadyFinished
        })
    }

    fn has_pending_background_work(&self) -> bool {
        self.shared.normalizer.lock().has_live_tasks()
    }

    async fn close(&self) -> DriverResult<()> {
        let mut process = self.shared.process.lock().await;
        match process.as_mut() {
            Some(process) => {
                // Closing stdin is the shutdown signal; the read loop ends
                // the stream once the CLI exits.
                if let Some(mut stdin) = process.stdin.take() {
                    let _ = stdin.shutdown().await;
                }
            }
            None => {
                self.shared.dead.store(true, Ordering::Release);
                self.shared.events.lock().take();
            }
        }
        Ok(())
    }
}

/// A provider-thread snapshot from the native transcript.
fn snapshot_from_history(
    shared: &Shared,
    thread: &ProviderThreadRef,
    history: &waku_protocol::model::ProviderSessionHistory,
) -> ThreadSnapshot {
    let now = shared.clock.now();
    let scope = thread.provider_thread_id.0;
    let mut row = ProviderThread {
        id: thread.provider_thread_id,
        driver: orch::DriverKind::Claude,
        provider_instance_id: shared.instance.instance_id.clone(),
        provider_session_id: None,
        app_thread_id: None,
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
    let thread_id = ThreadId(shared.ids.bind(scope, EntityKind::Node, "snapshot"));
    let mut turns = Vec::new();
    let mut messages = Vec::new();
    let mut turn_ids = HashMap::new();
    for turn in &history.turns {
        let ordinal = turn.turn_count as u32;
        let id = ProviderTurnId(shared.ids.bind_ordinal(
            scope,
            EntityKind::ProviderTurn,
            u64::from(ordinal),
        ));
        turn_ids.insert(turn.id, (id, ordinal));
        turns.push(ProviderTurn {
            id,
            provider_thread_id: thread.provider_thread_id,
            node_id: NodeId(
                shared
                    .ids
                    .bind_ordinal(scope, EntityKind::Node, u64::from(ordinal)),
            ),
            run_attempt_id: None,
            native_turn_ref: turn
                .provider_resume_at
                .as_ref()
                .map(|uuid| NativeRef::weak(uuid).to_provider_ref(orch::DriverKind::Claude)),
            ordinal,
            status: ProviderTurnStatus::Completed,
            started_at: Some(turn.started_at as i64 * 1000),
            completed_at: turn.completed_at.map(|at| at as i64 * 1000),
            token_usage: None,
            turn_token_usage: None,
        });
    }
    for (index, message) in history.messages.iter().enumerate() {
        let ordinal = message
            .turn_id
            .and_then(|turn| turn_ids.get(&turn))
            .map_or(0, |(_, ordinal)| *ordinal);
        messages.push(Message {
            id: MessageId(
                shared
                    .ids
                    .bind_ordinal(scope, EntityKind::Message, index as u64),
            ),
            thread_id,
            run_id: None,
            node_id: (ordinal > 0).then(|| {
                NodeId(
                    shared
                        .ids
                        .bind_ordinal(scope, EntityKind::Node, u64::from(ordinal)),
                )
            }),
            role: match message.role {
                waku_protocol::model::MessageRole::User => MessageRole::User,
                _ => MessageRole::Assistant,
            },
            created_by: match message.role {
                waku_protocol::model::MessageRole::User => Actor::User,
                _ => Actor::Agent,
            },
            creation_source: CreationSource::Provider,
            text: message.content.clone(),
            display_text: None,
            attachments: Vec::new(),
            context: Vec::new(),
            notification: None,
            sender_thread_id: None,
            streaming: false,
            created_at: message.created_at as i64 * 1000,
            updated_at: message.created_at as i64 * 1000,
        });
    }
    ThreadSnapshot {
        thread: row,
        turns,
        messages,
        pending_requests: Vec::new(),
    }
}

#[cfg(test)]
mod tests;

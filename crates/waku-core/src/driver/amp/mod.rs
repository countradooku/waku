//! Amp on `DriverV2` (RFC 0002 "Amp", #51).
//!
//! `ensure_thread` creates the native thread with a short-lived `amp
//! threads new` (channel `threads-new`); the session process is `amp
//! threads continue <id> --execute --stream-json-thinking
//! --stream-json-input …` (channel `main`), started with the first turn and
//! serving the thread's turns until stdin closes. A turn is a user message
//! line; steering is the same line with `"steer": true`.
//!
//! Amp has no interrupt on its stream: `interrupt_turn` sends SIGINT
//! through the transports (the one signal RFC 0002 allows) and retires the
//! process. The turn ends `Interrupted` and the thread stays reusable: the
//! next turn starts a new process with `threads continue`. Amp offers no
//! permission request, so only Full access runs, and it has no native fork
//! or rollback: the orchestrator hands the history to a new thread
//! instead (D-6). The driver wraps every context block in the shared
//! `WAKU_CONTEXT_V1` envelope.

mod normalize;
pub mod protocol;

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::oneshot;
use waku_protocol::orchestration::{self as orch, *};

use self::normalize::{Normalizer, Output, Scope};
pub use self::protocol::{AMP_PROTOCOL, AMP_PROVIDER};
use crate::driver::v2::{
    AdapterEvent, AdapterEventSender, Clock, DriverError, DriverFamily, DriverResult,
    DriverSession, DriverV2, EnsureThread, EntityKind, ForkRequest, FrameDirection, IdAllocator,
    InstanceConfig, Interrupt, NativeRef, NotReadyReason, OpenedSession, ProcessSpec,
    ProtocolReason, ProviderThreadRef, RawFrameLog, ResumeThread, Rollback, RollbackOutcome,
    RuntimeResponse, Selection, SessionContext, Steer, SteerRejectReason, ThreadSnapshot,
    TransitionPlan, TransportFactory, TurnAccepted, TurnOutcome, TurnStart, adapter_channel,
    wrap_synthetic_context,
};

/// `amp threads new` and `amp threads export` are one-shot commands.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(60);

/// The capabilities this driver really has (RFC 0002 matrix, Amp column).
pub fn amp_capabilities() -> Capabilities {
    let mut caps = Capabilities::default();
    caps.sessions.provider_switching_via_handoff = true;
    caps.threads.create_empty_thread = true;
    caps.threads.read_snapshot = true;
    caps.threads.native_thread_id = true;
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
    caps.tools.mcp_tools = true;
    caps.context.accepts_synthetic_user_context = true;
    caps.context.can_consume_handoff_summaries = true;
    caps.context.delta_handoff = true;
    caps.context.full_thread_handoff = true;
    caps.checkpointing.app_can_checkpoint_filesystem = true;
    caps.checkpointing.nested_scopes = true;
    caps.checkpointing.provider_can_read_conversation_snapshot = true;
    caps.identity.thread_ids = RefStrength::Strong;
    caps.identity.turn_ids = RefStrength::None;
    caps.identity.item_ids = RefStrength::Weak;
    caps.identity.request_ids = RefStrength::None;
    caps.runtime_policy.enforcement = Enforcement::ClientBoundary;
    caps.runtime_policy.supported_modes = vec![RuntimeMode::FullAccess];
    caps.extras.agent_presets = true;
    caps
}

/// An instance of `amp` found on the search path.
pub fn amp_instance() -> InstanceConfig {
    let mut cfg = InstanceConfig::new(AMP_PROVIDER, orch::DriverKind::Amp);
    cfg.binary = "amp".into();
    cfg
}

#[derive(Clone, Copy, Debug, Default)]
pub struct AmpDriver;

impl AmpDriver {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl DriverV2 for AmpDriver {
    fn kind(&self) -> DriverFamily {
        DriverFamily::Amp
    }

    fn capabilities(&self, _cfg: &InstanceConfig) -> Capabilities {
        amp_capabilities()
    }

    fn plan_selection_transition(
        &self,
        _session_caps: &Capabilities,
        from: &Selection,
        to: &Selection,
    ) -> TransitionPlan {
        let mut plans = Vec::new();
        // Mode, effort and tier are all launch arguments.
        if from.model != to.model
            || from.reasoning_effort != to.reasoning_effort
            || from.service_tier != to.service_tier
            || from.context_window != to.context_window
            || from.agent_preset != to.agent_preset
        {
            plans.push(TransitionPlan::RestartSession);
        }
        if from.runtime_mode != to.runtime_mode {
            plans.push(if to.runtime_mode == RuntimeMode::FullAccess {
                TransitionPlan::RestartSession
            } else {
                TransitionPlan::Reject {
                    reason: "Amp runs in Full access only".to_owned(),
                }
            });
        }
        if from.interaction_mode != to.interaction_mode {
            plans.push(TransitionPlan::Reject {
                reason: "Amp has no plan mode".to_owned(),
            });
        }
        TransitionPlan::combine(plans)
    }

    async fn open_session(&self, ctx: SessionContext) -> DriverResult<OpenedSession> {
        if ctx.selection.runtime_mode != RuntimeMode::FullAccess {
            return Err(DriverError::Provider(ProviderFailure::new(
                FailureClass::ValidationError,
                "Amp runs in Full access only.",
            )));
        }
        let capabilities = amp_capabilities();
        let (events, receiver) = adapter_channel();
        let shared = Arc::new(Shared {
            normalizer: Mutex::new(Normalizer::new(Scope {
                ids: ctx.ids.clone(),
                clock: ctx.clock.clone(),
            })),
            session_id: ctx.provider_session_id,
            ids: ctx.ids,
            clock: ctx.clock,
            raw_log: ctx.raw_log,
            transports: ctx.transports,
            instance: ctx.instance,
            cwd: ctx.cwd,
            process: tokio::sync::Mutex::new(None),
            generation: AtomicU64::new(0),
            retiring: Mutex::new(HashSet::new()),
            events: Mutex::new(Some(events)),
            dead: AtomicBool::new(false),
            capabilities: capabilities.clone(),
        });
        Ok(OpenedSession {
            session: Arc::new(AmpSession { shared }),
            events: receiver,
            capabilities,
        })
    }
}

struct Process {
    stdin: Option<Box<dyn AsyncWrite + Send + Unpin>>,
    thread_id: String,
    generation: u64,
    pid: Option<u32>,
    exited: Option<oneshot::Receiver<()>>,
}

struct Shared {
    normalizer: Mutex<Normalizer>,
    session_id: ProviderSessionId,
    ids: Arc<dyn IdAllocator>,
    clock: Arc<dyn Clock>,
    raw_log: Arc<dyn RawFrameLog>,
    transports: Arc<dyn TransportFactory>,
    instance: InstanceConfig,
    cwd: PathBuf,
    process: tokio::sync::Mutex<Option<Process>>,
    generation: AtomicU64,
    /// Generations ended on purpose (an interrupt, a thread switch), whose
    /// exit is not a loss.
    retiring: Mutex<HashSet<u64>>,
    events: Mutex<Option<AdapterEventSender>>,
    /// The session's process ended unexpectedly, or the session closed.
    dead: AtomicBool,
    capabilities: Capabilities,
}

impl Shared {
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
                detail: "Amp is not running".to_owned(),
            });
        };
        stdin.write_all(&line).await?;
        stdin.flush().await?;
        Ok(())
    }

    /// Ends a process on purpose: its exit settles nothing as lost.
    async fn retire(&self, mut process: Process, wait: bool) {
        self.retiring.lock().insert(process.generation);
        if let Some(mut stdin) = process.stdin.take() {
            let _ = stdin.shutdown().await;
        }
        if wait && let Some(exited) = process.exited.take() {
            let _ = exited.await;
        }
    }

    /// Starts the process for `thread_id` unless it already serves it.
    async fn ensure_process(
        self: &Arc<Self>,
        thread_id: &str,
        selection: &Selection,
    ) -> DriverResult<()> {
        if self.dead.load(Ordering::Acquire) {
            return Err(DriverError::SessionLost {
                detail: "the Amp session ended".to_owned(),
            });
        }
        let mut process = self.process.lock().await;
        if process
            .as_ref()
            .is_some_and(|running| running.thread_id == thread_id)
        {
            return Ok(());
        }
        if let Some(old) = process.take() {
            self.retire(old, true).await;
        }
        let spec = ProcessSpec {
            program: self.instance.binary.clone(),
            args: protocol::launch_args(selection, thread_id),
            env: self.instance.env.clone(),
            cwd: self.cwd.clone(),
        };
        let transport = self.transports.spawn_stdio("main", spec).await?;
        let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        let (exited_sender, exited) = oneshot::channel();
        *process = Some(Process {
            stdin: Some(transport.stdin),
            thread_id: thread_id.to_owned(),
            generation,
            pid: transport.pid,
            exited: Some(exited),
        });
        drop(process);
        drain_stderr(transport.stderr, self.raw_log.clone());
        tokio::spawn(read_loop(
            self.clone(),
            generation,
            transport.stdout,
            transport.exit,
            exited_sender,
        ));
        Ok(())
    }

    /// Runs a one-shot `amp` command (`threads new`, `threads export`) on its
    /// own channel and returns its stdout.
    async fn command(&self, channel: &str, args: Vec<String>) -> DriverResult<String> {
        let mut transport = self
            .transports
            .spawn_stdio(
                channel,
                ProcessSpec {
                    program: self.instance.binary.clone(),
                    args,
                    env: self.instance.env.clone(),
                    cwd: self.cwd.clone(),
                },
            )
            .await?;
        // Nothing to say on stdin: closing it lets the command finish.
        let _ = transport.stdin.shutdown().await;
        drain_stderr(transport.stderr, self.raw_log.clone());
        let mut stdout = String::new();
        let read = async {
            transport.stdout.read_to_string(&mut stdout).await?;
            transport.exit.await
        };
        let status = tokio::time::timeout(COMMAND_TIMEOUT, read)
            .await
            .map_err(|_| DriverError::Protocol {
                reason: ProtocolReason::Timeout,
                detail: format!("amp {channel} timed out"),
            })??;
        if !status.success {
            return Err(DriverError::Protocol {
                reason: ProtocolReason::UnexpectedFrame,
                detail: format!("amp {channel} exited with {:?}", status.code),
            });
        }
        Ok(stdout)
    }
}

/// Amp writes diagnostics on stderr; an unread pipe would eventually block
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
    generation: u64,
    stdout: Box<dyn AsyncRead + Send + Unpin>,
    exit: futures_util::future::BoxFuture<'static, std::io::Result<crate::driver::v2::ProcessExit>>,
    exited: oneshot::Sender<()>,
) {
    let mut lines = BufReader::new(stdout).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
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
            Some(output) => shared.emit(output.events).await,
            None => shared
                .raw_log
                .record("main", FrameDirection::Inbound, &line),
        }
    }
    let status = exit.await;
    let retired = shared.retiring.lock().remove(&generation);
    let events = {
        let mut normalizer = shared.normalizer.lock();
        let mut output = Output::default();
        let interrupted = normalizer
            .turn
            .as_ref()
            .is_some_and(|turn| turn.interrupt_requested);
        if retired || interrupted {
            // Ended on purpose (SIGINT, a thread switch): an interrupted turn
            // settles here and the thread stays reusable.
            normalizer.finish_turn(TurnOutcome::Interrupted, None, &mut output);
        } else if normalizer.turn.is_some() {
            let detail = match &status {
                Ok(exit) if exit.success => "Amp exited.".to_owned(),
                Ok(exit) => format!("Amp exited ({:?}).", exit.code),
                Err(error) => format!("Amp failed: {error}"),
            };
            normalizer.finish_turn(
                TurnOutcome::Failed {
                    failure: ProviderFailure::new(FailureClass::TransportError, detail.clone())
                        .with_code("process_exit"),
                },
                Some(detail),
                &mut output,
            );
        }
        let mut events = normalizer.flush();
        events.extend(output.events);
        events
    };
    shared.emit(events).await;
    {
        let mut process = shared.process.lock().await;
        if process
            .as_ref()
            .is_some_and(|process| process.generation == generation)
        {
            process.take();
        }
    }
    if !retired {
        // The thread's process is gone: closed by `close()` or lost. The
        // stream ends; the manager reopens the session if needed.
        shared.dead.store(true, Ordering::Release);
        shared.events.lock().take();
    }
    let _ = exited.send(());
}

struct AmpSession {
    shared: Arc<Shared>,
}

/// The text Amp receives for a turn: every context block in the shared
/// envelope (Amp has no other context channel), then the message with
/// attachment paths mentioned.
fn turn_text(req: &TurnStart) -> String {
    let mut text = req.message.text.clone();
    for attachment in &req.message.attachments {
        text.push_str(&format!(
            "\n\n[Attached file: {} at {}]",
            attachment.name,
            attachment.path.display()
        ));
    }
    wrap_synthetic_context(&req.app_context, &text)
}

#[async_trait]
impl DriverSession for AmpSession {
    fn capabilities(&self) -> &Capabilities {
        &self.shared.capabilities
    }

    async fn ensure_thread(&self, req: EnsureThread) -> DriverResult<ProviderThreadRef> {
        let shared = &self.shared;
        let stdout = shared
            .command("threads-new", vec!["threads".to_owned(), "new".to_owned()])
            .await?;
        let thread_id = stdout
            .lines()
            .find_map(protocol::thread_id_from_line)
            .ok_or_else(|| DriverError::Protocol {
                reason: ProtocolReason::UnexpectedFrame,
                detail: "amp threads new printed no thread id".to_owned(),
            })?;
        let provider_thread_id = match &req.existing {
            Some(existing) => existing.provider_thread_id,
            None => ProviderThreadId(shared.ids.bind(
                shared.session_id.0,
                EntityKind::ProviderThread,
                &thread_id,
            )),
        };
        Ok(ProviderThreadRef {
            provider_thread_id,
            native: NativeRef::strong(&thread_id),
            cursor: Some(protocol::cursor(&thread_id)),
        })
    }

    async fn resume_thread(&self, req: ResumeThread) -> DriverResult<ProviderThreadRef> {
        // No I/O: the next turn starts `threads continue <id>`.
        if !req.thread.is_bound() {
            return Err(DriverError::NotReady(NotReadyReason::ThreadMissing));
        }
        Ok(req.thread)
    }

    async fn start_turn(&self, req: TurnStart) -> DriverResult<TurnAccepted> {
        let shared = &self.shared;
        if shared.normalizer.lock().turn.is_some() {
            return Err(DriverError::NotReady(NotReadyReason::PreviousTurnActive));
        }
        if !req.thread.is_bound() {
            return Err(DriverError::NotReady(NotReadyReason::ThreadMissing));
        }
        shared
            .ensure_process(&req.thread.native.id, &req.selection)
            .await?;
        let text = turn_text(&req);
        let (reference, events) =
            shared
                .normalizer
                .lock()
                .begin_turn(req.run, &req.thread, req.provider_turn_ordinal);
        if let Err(error) = shared.write(&protocol::user_message(&text, false)).await {
            shared.normalizer.lock().abandon_turn(req.run.attempt_id);
            return Err(error);
        }
        shared.emit(events).await;
        Ok(TurnAccepted {
            provider_turn: Some(reference),
        })
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
        // The running turn absorbs the message at its next interruption
        // point; one `end_turn` settles both.
        self.shared
            .write(&protocol::user_message(&req.message.text, true))
            .await
    }

    async fn interrupt_turn(&self, req: Interrupt) -> DriverResult<()> {
        let shared = &self.shared;
        if !shared.normalizer.lock().mark_interrupt(req.run.attempt_id) {
            // Nothing of this attempt is running; its terminal was sent.
            return Ok(());
        }
        let mut process = shared.process.lock().await;
        let Some(running) = process.take() else {
            drop(process);
            let mut output = Output::default();
            shared
                .normalizer
                .lock()
                .finish_turn(TurnOutcome::Interrupted, None, &mut output);
            shared.emit(output.events).await;
            return Ok(());
        };
        drop(process);
        // Amp has no stream interrupt: SIGINT ends the run and the process.
        // Its exit settles the turn `Interrupted`; the thread is reused by
        // the next turn's `threads continue`. Retired before the signal, so
        // the exit can never read as a lost session.
        shared.retiring.lock().insert(running.generation);
        let signalled = shared.transports.interrupt("main", running.pid).await;
        if let Err(error) = &signalled {
            shared.raw_log.record(
                "main",
                FrameDirection::Note,
                &format!("SIGINT failed ({error}); closing stdin instead"),
            );
        }
        shared.retire(running, false).await;
        Ok(())
    }

    async fn respond(&self, _req: RuntimeResponse) -> DriverResult<()> {
        // Amp asks nothing over the stream.
        Err(DriverError::Unsupported {
            operation: "respond",
        })
    }

    async fn read_snapshot(&self, thread: &ProviderThreadRef) -> DriverResult<ThreadSnapshot> {
        let shared = &self.shared;
        let stdout = shared
            .command(
                "export",
                vec![
                    "threads".to_owned(),
                    "export".to_owned(),
                    thread.native.id.clone(),
                ],
            )
            .await?;
        let history = tokio::task::spawn_blocking(move || {
            let export: Value = serde_json::from_str(&stdout)?;
            crate::amp_session::history_from_export(&export)
        })
        .await
        .map_err(|error| DriverError::Io(std::io::Error::other(error)))?
        .map_err(|error| DriverError::Protocol {
            reason: ProtocolReason::Decode,
            detail: error.to_string(),
        })?;
        Ok(crate::driver::history::snapshot_from_history(
            shared.ids.as_ref(),
            shared.clock.now(),
            orch::DriverKind::Amp,
            &shared.instance,
            thread,
            &history,
        ))
    }

    async fn rollback(&self, _req: Rollback) -> DriverResult<RollbackOutcome> {
        // D-6: the orchestrator rolls back with a fresh thread and a handoff.
        Err(DriverError::Unsupported {
            operation: "rollback",
        })
    }

    async fn fork(&self, _req: ForkRequest) -> DriverResult<ProviderThreadRef> {
        // D-6: portable forks are the orchestrator's handoff.
        Err(DriverError::Unsupported { operation: "fork" })
    }

    async fn close(&self) -> DriverResult<()> {
        let mut process = self.shared.process.lock().await;
        match process.as_mut() {
            // Closing stdin is the shutdown signal; the read loop ends the
            // stream once Amp exits.
            Some(process) => {
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

#[cfg(test)]
mod tests;

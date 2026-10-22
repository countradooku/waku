//! `DriverV2`: async provider drivers that emit normalized adapter events and
//! declare per-session capabilities (RFC 0002, #44).
//!
//! ```text
//! DriverV2 (one per transport family, no session state)
//!   └─ open_session(SessionContext) ─► OpenedSession { session, events, capabilities }
//!        DriverSession (one actor per session; every method takes &self)
//!        events: bounded channel of AdapterEvent ─► ProviderSessionManager
//!                                                    ├─ CommandOutputDelta ─► OutputSink
//!                                                    ├─ live fan-out (subscribers)
//!                                                    └─ persist throttle ─► Orchestrator::ingest
//! ```
//!
//! Drivers never spawn processes or open sockets themselves: they go through
//! [`SessionContext::transports`], which replay replaces (#45).
//!
//! The normalized events, run references and the selection-transition plan
//! are shared with `waku-orchestrator`, whose pure `decide_ingest` reads them;
//! they are re-exported here so a driver imports one module.

// `async-trait` marks its boxed futures `#[must_use]` on top of the futures'
// own attribute, which newer clippy reports as `double_must_use`.
#![allow(clippy::double_must_use)]

pub mod coalescer;
pub mod fake;
pub mod fake_agent;
pub mod http;
pub mod ids;
pub mod manager;
pub mod replay;
pub mod transport;

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc};
use waku_protocol::model::ProviderResumeCursor;
use waku_protocol::orchestration::{
    self as orch, Capabilities, InteractionMode, JsonText, MessageId, ModelSelection, NodeId,
    ProviderFailure, ProviderInstanceId, ProviderSessionId, ProviderThreadId, RunId, RuntimeMode,
    RuntimeRequestId, ThreadGoal, ThreadId, UserInputAnswer,
};

pub use waku_orchestrator::command::ThreadSnapshot;
pub use waku_orchestrator::provider::{
    ADAPTER_EVENT_CAPACITY, AdapterEvent, BackgroundWorkItem, NativeRef, ProviderTurnRef, RunRef,
    ThreadDisposition, TurnOutcome, TurnTerminal,
};
pub use waku_orchestrator::{Clock, TransitionPlan};

pub use self::ids::{EntityKind, IdAllocator, SeqIdAllocator, UuidIdAllocator};
pub use self::transport::{
    Downlink, HttpMethod, HttpResponse, HttpTransport, ProcessExit, ProcessSpec,
    ProcessTransportFactory, StdioTransport, TransportFactory,
};

/// The runtime every driver session runs on: drivers are async (RFC 0002
/// D-2) and the daemon's ports are not. Created on first use. Its threads
/// unblock SIGCHLD: agents spawned from them inherit the signal mask and
/// must be able to reap their children.
pub fn runtime() -> tokio::runtime::Handle {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_name("waku-driver-v2")
                .on_thread_start(|| {
                    let _ = crate::command_env::unblock_sigchld_for_current_thread();
                })
                .enable_all()
                .build()
                .expect("the driver runtime starts")
        })
        .handle()
        .clone()
}

pub type DriverResult<T> = Result<T, DriverError>;
pub type AdapterEventSender = mpsc::Sender<AdapterEvent>;
pub type AdapterEventReceiver = mpsc::Receiver<AdapterEvent>;

/// The bounded adapter-event channel every driver uses.
pub fn adapter_channel() -> (AdapterEventSender, AdapterEventReceiver) {
    mpsc::channel(ADAPTER_EVENT_CAPACITY)
}

/// A transport family. One `DriverV2` serves every flavor of its family
/// (Cursor, Grok, Kimi and Fx are all `Acp`); `InstanceConfig::provider`
/// names the flavor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DriverFamily {
    Codex,
    Claude,
    Acp,
    OpenCode,
    Pi,
    Amp,
    DeepSeek,
    /// The in-repo fake driver that proves the trait (#44, #45).
    Test,
}

/// One implementation per transport family. Holds no per-session state.
#[async_trait]
pub trait DriverV2: Send + Sync + 'static {
    fn kind(&self) -> DriverFamily;

    /// The most this instance can do, before any session exists. Pure: no
    /// I/O, no process spawn.
    fn capabilities(&self, cfg: &InstanceConfig) -> Capabilities;

    /// Pure. Decides how a selection change reaches a live session.
    fn plan_selection_transition(
        &self,
        session_caps: &Capabilities,
        from: &Selection,
        to: &Selection,
    ) -> TransitionPlan;

    /// Starts or attaches the native runtime and completes the handshake.
    /// Returns once the session accepts `ensure_thread` / `resume_thread`.
    async fn open_session(&self, ctx: SessionContext) -> DriverResult<OpenedSession>;
}

pub struct OpenedSession {
    pub session: Arc<dyn DriverSession>,
    /// Single consumer. The `ProviderSessionManager` fans it out.
    pub events: AdapterEventReceiver,
    /// Negotiated after the handshake. Never stronger than
    /// `DriverV2::capabilities`. Persisted on the `ProviderSession`.
    pub capabilities: Capabilities,
}

/// One live provider session. Each is an actor: methods send a command and
/// await its reply, so they all take `&self`.
#[async_trait]
pub trait DriverSession: Send + Sync {
    fn capabilities(&self) -> &Capabilities;

    async fn ensure_thread(&self, req: EnsureThread) -> DriverResult<ProviderThreadRef>;
    async fn resume_thread(&self, req: ResumeThread) -> DriverResult<ProviderThreadRef>;

    /// Resolves when the provider accepted the input. The end arrives as
    /// exactly one `AdapterEvent::TurnTerminal` for `req.run`.
    async fn start_turn(&self, req: TurnStart) -> DriverResult<TurnAccepted>;

    /// Called only when `caps.turns.active_steering`. `Err(SteerRejected)`
    /// when the turn has already settled.
    async fn steer_turn(&self, req: Steer) -> DriverResult<()>;

    /// Resolves when the interrupt is delivered. The terminal arrives later.
    async fn interrupt_turn(&self, req: Interrupt) -> DriverResult<()>;

    /// Approvals, structured questions and dismissals.
    async fn respond(&self, req: RuntimeResponse) -> DriverResult<()>;

    async fn read_snapshot(&self, thread: &ProviderThreadRef) -> DriverResult<ThreadSnapshot>;
    async fn rollback(&self, req: Rollback) -> DriverResult<RollbackOutcome>;
    async fn fork(&self, req: ForkRequest) -> DriverResult<ProviderThreadRef>;

    /// The in-session half of `TransitionPlan::ApplyInSession`.
    async fn apply_selection(&self, _to: &Selection) -> DriverResult<()> {
        Err(DriverError::Unsupported {
            operation: "apply_selection",
        })
    }

    // Optional. Each one is gated by a capability.
    async fn compact(&self, _thread: &ProviderThreadRef) -> DriverResult<()> {
        Err(DriverError::Unsupported {
            operation: "compact",
        })
    }
    async fn inject_history(&self, _history: HistoryInjection) -> DriverResult<()> {
        Err(DriverError::Unsupported {
            operation: "inject_history",
        })
    }
    async fn goal(&self, _op: GoalRequest) -> DriverResult<()> {
        Err(DriverError::Unsupported { operation: "goal" })
    }
    async fn stop_background_work(&self, _work: BackgroundWorkRef) -> DriverResult<()> {
        Err(DriverError::Unsupported {
            operation: "stop_background_work",
        })
    }
    /// Re-report the background work the provider still runs, and what of
    /// the reported work ended, as `BackgroundWorkUpdated` events. Drivers
    /// that report every change as it happens have nothing to do.
    async fn refresh_background_work(&self) -> DriverResult<()> {
        Ok(())
    }
    async fn stop_subagent(&self, _subagent: SubagentRef) -> DriverResult<SubagentStopOutcome> {
        Err(DriverError::Unsupported {
            operation: "stop_subagent",
        })
    }
    async fn unload_thread(&self, _thread: &ProviderThreadRef) -> DriverResult<()> {
        Ok(())
    }
    fn has_pending_background_work(&self) -> bool {
        false
    }
    /// Debug-build extension for Computer Use.
    fn computer_use(&self) -> Option<&dyn ComputerUseControl> {
        None
    }

    /// Graceful shutdown: close stdin, or unsubscribe for adopted services.
    /// Never terminates a service Waku did not start.
    async fn close(&self) -> DriverResult<()>;
}

/// Today's `run_computer_tool`, `reject_computer_tool` and
/// `cancel_computer_use`, unchanged.
pub trait ComputerUseControl: Send + Sync {
    fn run_computer_tool(&self, request: crate::computer_use::ComputerToolRequest);
    fn reject_computer_tool(
        &self,
        request: crate::computer_use::ComputerToolRequest,
        reason: String,
    );
    fn cancel_computer_use(&self);
}

// -- session context -------------------------------------------------------------

/// One configured provider instance (#80).
#[derive(Clone, Debug, PartialEq)]
pub struct InstanceConfig {
    pub instance_id: ProviderInstanceId,
    /// The flavor inside the driver family (Cursor/Grok/Kimi/Fx for Acp,
    /// Pi/OhMyPi for Pi).
    pub provider: orch::DriverKind,
    pub binary: PathBuf,
    pub env: Vec<(String, String)>,
    /// CODEX_HOME, CLAUDE_CONFIG_DIR, Pi's agent dir, … `None` = the CLI default.
    pub provider_home: Option<PathBuf>,
    /// Probed at instance start. Capabilities may depend on it.
    pub cli_version: Option<String>,
    pub computer_use_enabled: bool,
    /// Driver-specific settings, validated by the driver.
    pub extra: serde_json::Value,
}

impl InstanceConfig {
    pub fn new(instance_id: impl Into<String>, provider: orch::DriverKind) -> Self {
        Self {
            instance_id: ProviderInstanceId::new(instance_id),
            provider,
            binary: PathBuf::new(),
            env: Vec::new(),
            provider_home: None,
            cli_version: None,
            computer_use_enabled: false,
            extra: serde_json::Value::Null,
        }
    }
}

/// Where command output goes (RFC 0003's hub). Never persisted.
pub trait OutputSink: Send + Sync {
    fn command_output(
        &self,
        thread_id: ThreadId,
        item: orch::TurnItemId,
        chunk: &[u8],
        replace: bool,
    );

    /// The command finished: what its turn item stores as `output` (the
    /// retained head and tail, RFC 0001 R12), or `None` when the sink kept
    /// nothing for it.
    fn finish(
        &self,
        _item: orch::TurnItemId,
        _exit_code: Option<i32>,
    ) -> Option<waku_orchestrator::streams::StoredOutput> {
        None
    }
}

/// The daemon's output hub: chunks reach `commandOutput` subscribers live,
/// and a finished command's retained output is stored in its item.
impl OutputSink for waku_orchestrator::streams::CommandOutputHub {
    fn command_output(
        &self,
        thread_id: ThreadId,
        item: orch::TurnItemId,
        chunk: &[u8],
        replace: bool,
    ) {
        let text = String::from_utf8_lossy(chunk);
        if replace {
            self.replace(thread_id, item, &text);
        } else {
            self.append(thread_id, item, &text);
        }
    }

    fn finish(
        &self,
        item: orch::TurnItemId,
        exit_code: Option<i32>,
    ) -> Option<waku_orchestrator::streams::StoredOutput> {
        waku_orchestrator::streams::CommandOutputHub::finish(self, item, exit_code)
    }
}

/// Drops all output.
#[derive(Clone, Copy, Debug, Default)]
pub struct NullOutput;

impl OutputSink for NullOutput {
    fn command_output(&self, _: ThreadId, _: orch::TurnItemId, _: &[u8], _: bool) {}
}

/// The rotating raw-frame log (RFC 0001: raw frames never go to SQLite).
/// Unknown frames and private error details go here.
pub trait RawFrameLog: Send + Sync {
    fn record(&self, channel: &str, direction: FrameDirection, frame: &str);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameDirection {
    Outbound,
    Inbound,
    Note,
}

/// Keeps nothing.
#[derive(Clone, Copy, Debug, Default)]
pub struct NullRawLog;

impl RawFrameLog for NullRawLog {
    fn record(&self, _: &str, _: FrameDirection, _: &str) {}
}

/// A cooperative shutdown signal shared by a session's tasks.
#[derive(Clone, Default)]
pub struct Shutdown {
    inner: Arc<(AtomicBool, Notify)>,
}

impl Shutdown {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.inner.0.store(true, Ordering::Release);
        self.inner.1.notify_waiters();
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.0.load(Ordering::Acquire)
    }

    pub async fn cancelled(&self) {
        while !self.is_cancelled() {
            let notified = self.inner.1.notified();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

impl fmt::Debug for Shutdown {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Shutdown")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

/// The thread-scoped app MCP server (#72, #84). Drivers that cannot inject it
/// must still start without it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpInjection {
    pub name: String,
    pub config: JsonText,
}

pub struct SessionContext {
    pub provider_session_id: ProviderSessionId,
    pub thread_id: ThreadId,
    pub instance: InstanceConfig,
    /// Canonicalized: symlinks resolved (macOS `/tmp` → `/private/tmp`).
    pub cwd: PathBuf,
    pub selection: Selection,
    /// Set when reopening a persisted session.
    pub resume: Option<ResumeState>,
    pub ids: Arc<dyn IdAllocator>,
    pub clock: Arc<dyn Clock>,
    /// Real processes and sockets in production; replay in tests.
    pub transports: Arc<dyn TransportFactory>,
    pub mcp: Option<McpInjection>,
    pub output: Arc<dyn OutputSink>,
    pub raw_log: Arc<dyn RawFrameLog>,
    pub shutdown: Shutdown,
}

pub struct ResumeState {
    pub session: orch::ProviderSession,
    pub threads: Vec<ProviderThreadRef>,
}

/// A provider thread: its app id, native id and the driver's opaque cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderThreadRef {
    pub provider_thread_id: ProviderThreadId,
    pub native: NativeRef,
    /// The existing `ProviderResumeCursor`, kept as the per-driver opaque
    /// cursor. Persisted in `ProviderThread.native_metadata.resume_cursor`.
    pub cursor: Option<ProviderResumeCursor>,
}

impl ProviderThreadRef {
    /// A row the orchestrator created that has no native thread yet.
    pub fn unbound(provider_thread_id: ProviderThreadId) -> Self {
        Self {
            provider_thread_id,
            native: NativeRef {
                id: String::new(),
                strength: orch::RefStrength::None,
            },
            cursor: None,
        }
    }

    pub fn is_bound(&self) -> bool {
        !self.native.id.is_empty()
    }

    /// Reads the reference back from a persisted provider thread row.
    pub fn from_row(row: &orch::ProviderThread) -> Self {
        let native = row
            .native_thread_ref
            .as_ref()
            .and_then(|reference| {
                Some(NativeRef {
                    id: reference.native_id.clone()?,
                    strength: reference.strength,
                })
            })
            .unwrap_or(NativeRef {
                id: String::new(),
                strength: orch::RefStrength::None,
            });
        let cursor = row
            .native_metadata
            .as_ref()
            .and_then(|metadata| metadata.resume_cursor.as_ref())
            .and_then(|cursor| serde_json::from_str(cursor.as_str()).ok());
        Self {
            provider_thread_id: row.id,
            native,
            cursor,
        }
    }

    /// Writes the native id and cursor onto a provider thread row.
    pub fn apply_to(&self, row: &mut orch::ProviderThread) {
        if self.is_bound() {
            row.native_thread_ref = Some(self.native.to_provider_ref(row.driver));
        }
        if let Some(cursor) = &self.cursor {
            let metadata = row.native_metadata.get_or_insert_with(Default::default);
            metadata.resume_cursor = serde_json::to_string(cursor).ok().map(JsonText);
        }
    }
}

/// User-chosen settings: today's `SessionOptions` plus agent preset and
/// interaction mode.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Selection {
    /// Never the sentinel "default".
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub service_tier: Option<String>,
    pub context_window: Option<String>,
    pub agent_preset: Option<String>,
    pub runtime_mode: RuntimeMode,
    pub interaction_mode: InteractionMode,
}

impl Selection {
    pub fn from_model(
        selection: &ModelSelection,
        runtime_mode: RuntimeMode,
        interaction_mode: InteractionMode,
    ) -> Self {
        Self {
            model: selection
                .model
                .clone()
                .filter(|model| !model.is_empty() && model != "default"),
            reasoning_effort: selection.reasoning_effort.clone(),
            service_tier: selection.service_tier.clone(),
            context_window: selection.context_window.clone(),
            agent_preset: selection.agent_preset.clone(),
            runtime_mode,
            interaction_mode,
        }
    }
}

pub struct EnsureThread {
    pub thread_id: ThreadId,
    pub title: Option<String>,
    pub selection: Selection,
    /// The app's provider-thread row to bind; `None` lets the driver bind a
    /// new id through `ctx.ids`.
    pub existing: Option<ProviderThreadRef>,
}

pub struct ResumeThread {
    pub thread: ProviderThreadRef,
    pub selection: Selection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TurnIntent {
    Prompt,
    SlashCommand { name: String },
    Goal(GoalRequest),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnMessage {
    pub message_id: MessageId,
    pub text: String,
    pub attachments: Vec<Attachment>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Attachment {
    pub name: String,
    pub mime: Option<String>,
    pub path: PathBuf,
    pub is_image: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextKind {
    System,
    Developer,
    SyntheticUser,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextBlock {
    pub key: String,
    pub kind: ContextKind,
    pub text: String,
}

/// The provider-neutral envelope synthetic-user context travels in (port
/// step 11), unless a driver declares a native context channel.
pub const CONTEXT_ENVELOPE: &str = "WAKU_CONTEXT_V1";

/// Wraps context blocks and the user's text in the shared envelope.
pub fn wrap_synthetic_context(blocks: &[ContextBlock], text: &str) -> String {
    if blocks.is_empty() {
        return text.to_owned();
    }
    let mut wrapped = format!("<{CONTEXT_ENVELOPE}>\n");
    for block in blocks {
        wrapped.push_str(&format!(
            "<context key=\"{}\">\n{}\n</context>\n",
            block.key, block.text
        ));
    }
    wrapped.push_str(&format!("</{CONTEXT_ENVELOPE}>\n\n{text}"));
    wrapped
}

pub struct TurnStart {
    pub run: RunRef,
    pub run_ordinal: u32,
    pub provider_turn_ordinal: u32,
    /// Set on a steering-restart or retry attempt, and on a run continuing
    /// one a daemon restart interrupted (#59).
    pub restart_continuation_of: Option<RunId>,
    pub thread: ProviderThreadRef,
    pub message: TurnMessage,
    pub selection: Selection,
    /// Handoff, fork context and app instructions. The driver picks the
    /// channel by kind and its `context` capabilities.
    pub app_context: Vec<ContextBlock>,
    pub intent: TurnIntent,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TurnAccepted {
    pub provider_turn: Option<ProviderTurnRef>,
}

pub struct Steer {
    pub run: RunRef,
    pub thread: ProviderThreadRef,
    pub provider_turn: Option<ProviderTurnRef>,
    pub message: TurnMessage,
}

pub struct Interrupt {
    pub run: RunRef,
    pub thread: ProviderThreadRef,
    pub provider_turn: Option<ProviderTurnRef>,
    /// A steering restart follows; the driver keeps the session.
    pub restart_follows: bool,
}

/// The reply to a runtime request.
pub struct RuntimeResponse {
    pub request_id: RuntimeRequestId,
    pub native: NativeRef,
    pub reply: RuntimeReply,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeReply {
    /// The provider's own option id, exactly as it was emitted.
    Decision {
        option_id: String,
    },
    Answers(Vec<UserInputAnswer>),
    Dismiss,
}

pub struct Rollback {
    pub thread: ProviderThreadRef,
    pub target: RollbackTarget,
    /// Provider turns in order, so drivers without native turn ids can count.
    pub turns: Vec<ProviderTurnRef>,
    pub turns_to_remove: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RollbackTarget {
    ThreadStart,
    AfterProviderTurn(ProviderTurnRef),
}

pub struct RollbackOutcome {
    /// The same thread, or its replacement when `replaced`.
    pub thread: ProviderThreadRef,
    pub replaced: bool,
    pub snapshot: Option<ThreadSnapshot>,
    pub retained_native_turn_ids: Option<Vec<String>>,
}

pub struct ForkRequest {
    pub source: ProviderThreadRef,
    pub at: ForkPoint,
    pub target_thread_id: ThreadId,
    pub owner_node: Option<NodeId>,
    pub selection: Option<Selection>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ForkPoint {
    Tip,
    AfterProviderTurn {
        turn: ProviderTurnRef,
        turns_to_remove: u32,
    },
    SubagentThread(NativeRef),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GoalRequest {
    Get,
    Set(ThreadGoal),
    Clear,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryInjection {
    pub thread: ProviderThreadId,
    pub blocks: Vec<ContextBlock>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackgroundWorkRef {
    pub provider_thread_id: ProviderThreadId,
    pub task_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubagentRef {
    pub node_id: NodeId,
    pub native_task_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubagentStopOutcome {
    Stopped,
    AlreadyFinished,
}

// -- errors ------------------------------------------------------------------------

#[derive(Debug)]
pub enum DriverError {
    Unsupported {
        operation: &'static str,
    },
    NotReady(NotReadyReason),
    /// The native runtime is gone; the manager reopens with `ResumeState`.
    SessionLost {
        detail: String,
    },
    SteerRejected {
        reason: SteerRejectReason,
    },
    Provider(ProviderFailure),
    /// Only `reason` reaches user-facing text; `detail` goes to the raw log.
    Protocol {
        reason: ProtocolReason,
        detail: String,
    },
    Io(std::io::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotReadyReason {
    Connecting,
    PreviousTurnActive,
    NoActiveTurn,
    ThreadMissing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SteerRejectReason {
    TurnSettled,
    NoActiveTurn,
    ProviderRefused,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolReason {
    PreviousTurnActive,
    AttachmentUnreadable,
    UnexpectedFrame,
    Timeout,
    Decode,
}

impl fmt::Display for DriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported { operation } => {
                write!(formatter, "{operation} is not supported by this provider")
            }
            Self::NotReady(reason) => write!(formatter, "session not ready: {reason:?}"),
            Self::SessionLost { .. } => formatter.write_str("provider session lost"),
            Self::SteerRejected { reason } => write!(formatter, "steer rejected: {reason:?}"),
            Self::Provider(failure) => {
                write!(
                    formatter,
                    "provider failure ({}): {}",
                    failure.class, failure.message
                )
            }
            Self::Protocol { reason, .. } => write!(formatter, "protocol error ({reason:?})"),
            Self::Io(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for DriverError {}

impl From<std::io::Error> for DriverError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl DriverError {
    /// The user-safe failure a run records for this error. Unclassified
    /// errors are `Unknown`, never `ProviderError` (RFC 0002 "Errors").
    pub fn to_failure(&self) -> ProviderFailure {
        use orch::FailureClass;
        match self {
            Self::Provider(failure) => failure.clone(),
            Self::Unsupported { .. } => {
                ProviderFailure::new(FailureClass::ValidationError, self.to_string())
                    .with_code("unsupported")
            }
            Self::NotReady(_) => {
                ProviderFailure::new(FailureClass::Unknown, self.to_string()).with_code("not_ready")
            }
            Self::SessionLost { .. } | Self::Io(_) => ProviderFailure::new(
                FailureClass::TransportError,
                "The provider process stopped.",
            )
            .with_code("session_lost"),
            Self::SteerRejected { .. } => {
                ProviderFailure::new(FailureClass::Unknown, self.to_string())
                    .with_code("steer_rejected")
            }
            Self::Protocol { reason, .. } => {
                ProviderFailure::new(FailureClass::Unknown, format!("Protocol error: {reason:?}"))
                    .with_code("protocol")
            }
        }
    }
}

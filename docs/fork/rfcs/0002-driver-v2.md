# RFC 0002: `DriverV2`, normalized adapter events and capability groups

- **Status:** Accepted (pending merge of Phase 0 PR). Tracked in [#12](https://github.com/countradooku/waku/issues/12).
- **Implements:** [#44](https://github.com/countradooku/waku/issues/44) (trait, capabilities, session manager) and [#45](https://github.com/countradooku/waku/issues/45) (replay fixtures), plus the Phase 2 driver ports [#46](https://github.com/countradooku/waku/issues/46) Codex, [#47](https://github.com/countradooku/waku/issues/47) Claude, [#48](https://github.com/countradooku/waku/issues/48) ACP family, [#49](https://github.com/countradooku/waku/issues/49) OpenCode, [#50](https://github.com/countradooku/waku/issues/50) Pi / Oh My Pi, [#51](https://github.com/countradooku/waku/issues/51) Amp and [#52](https://github.com/countradooku/waku/issues/52) DeepSeek Harness.
- **Reference:** T3 `packages/provider-core/src/server/ProviderAdapter.ts`, `packages/contracts/src/orchestrationV2.ts` (capabilities, replay transcript), `packages/provider-testing/src/replayTranscript.ts`, `packages/effect-codex-app-server/src/replay.ts` ([audit](../audit/t3-providers.md) §1–2, [details](../audit/t3-provider-adapters-detail.md)).
- **Related:** [RFC 0001](0001-orchestrator.md) (orchestrator, entity model, effects), [RFC 0003](0003-protocol-v8.md) (protocol v8, command output hub), [#18](https://github.com/countradooku/waku/issues/18) (socket layer off 25 ms polling).

## Problem

Waku drivers implement `DriverControl` (prompt, steer, cancel, respond, respond_user_input, goal, apply_options, rollback, fork) and emit `DriverEvent`s.

- The desktop interprets those events itself.
- Capability flags such as "can rewind" are static on `ProviderKind` (`supports_conversation_rollback`, `supports_conversation_fork`, `supports_model_discovery`).
- Rollback and fork are split between the drivers and `daemon.rs` (`fork_provider_session`, `rewind_provider_response`), with per-provider `match` arms.
- Each driver runs 2–4 OS threads (writer, reader, stderr, waiter) and blocks on `recv_timeout` for request/response pairs.
- Nothing can be tested without the real CLI.

The orchestrator (RFC 0001) needs drivers that emit app-level entity updates and declare per-session capabilities, so policy, not provider names, decides behaviour.

## Decisions

| # | Decision |
|---|---|
| D-1 | `DriverV2` and `DriverSession` are object-safe async traits (`async-trait`). All session methods take `&self`; each session is an actor task. |
| D-2 | The daemon runs on a tokio multi-thread runtime, shared with the #18 socket rewrite. Drivers use tokio process, I/O and timers. `!Send` SDKs run on a dedicated thread with a current-thread runtime. |
| D-3 | Claude stays on the CLI's stream-json control protocol, spoken directly from Rust. No Agent SDK sidecar. |
| D-4 | Capabilities are the 12 T3 groups with T3's field names in snake_case, plus a 13th `extras` group for Waku-only switches. Every field defaults to the weakest value. |
| D-5 | Rollback can replace the provider thread. `RollbackOutcome` says whether it did. Five of the seven drivers roll back by forking. |
| D-6 | Portable forks (Cursor's and Amp's prompt envelopes) move out of the drivers into the orchestrator's handoff policy. Those drivers declare `fork = false`. |
| D-7 | ACP declares no active steering. The orchestrator steers by interrupt and restart, as T3 does. |
| D-8 | Replay fixtures use T3's NDJSON record shapes (`transcript_start`, `expect_outbound`, `emit_inbound`, `runtime_exit`, `afterMs`, `<workspace>`, `<any>`), plus an optional `channel` field for side processes. |
| D-9 | Each port replaces its driver in place. Until the orchestrator cutover, a generic `V1Bridge` (built in #44) runs any `DriverSession` behind the old `DriverControl` interface. |

## Threading and async model

**Decision (D-1, D-2).** The daemon moves to one tokio multi-thread runtime, the same one #18 introduces for the socket layer. Drivers are async and run on it.

Rules for driver code:

1. **One actor per session.** `open_session` spawns one task that owns the native connection: stdin writer, request-id counter, pending-request map and stream state. Trait methods send a command over an `mpsc` channel and await a `oneshot` reply. This keeps today's single-writer invariant (Codex's writer thread, OpenCode's worker) without locks.
2. **Processes.** Use `tokio::process::Command` with piped stdio. Shut down by closing stdin, never by a signal. The only exception is Amp's interrupt (see its section). Do not set `kill_on_drop`; `close()` owns shutdown.
3. **Timeouts.** Use `tokio::time::timeout`. Replay tests run with paused time, so timeouts are deterministic. No `recv_timeout`, `std::thread::sleep` or `block_on` on runtime threads.
4. **Blocking work.** Parsing Claude JSONL, Pi session files or Grok session directories goes through `spawn_blocking`.
5. **`!Send` SDKs.** The `agent-client-protocol` crate needs a local spawner. The ACP actor runs on a dedicated OS thread with a current-thread runtime and a `LocalSet`, as `acp.rs` does today with `smol::block_on`. The `DriverSession` handle stays `Send + Sync` and talks to it over channels.
6. **Backpressure.** The event channel is bounded (`ADAPTER_EVENT_CAPACITY = 1024`). A slow sink slows the read of the provider's pipe instead of growing memory. `CommandOutputDelta` goes to a separate output channel (`OutputSink`) that may coalesce and never blocks the reader.
7. **Text coalescing.** Text and reasoning deltas pass through the shared `DeltaCoalescer` in `driver::support`. It emits a `MessageUpdated` / `TurnItemUpdated` snapshot at most every 16 ms per item. It flushes before any other event for the same turn and before `TurnTerminal`.

Why not keep blocking threads:
- Today's model needs 3–4 OS threads per live session. Background work pins sessions for up to 4 h (#44), so idle sessions would hold hundreds of threads.
- Request/response pairs block on `recv_timeout`.
- Replay tests need virtual time, which tokio's paused clock provides.
- #18 replaces the 25 ms polling sockets and the thread-per-request handler with an evented loop. RFC 0001's effect workers need timers and leases. One runtime serves all three, and the session manager can await driver calls directly from effect workers.

Why tokio and not smol:
- tokio provides `process`, `time::pause`, `tokio-tungstenite` (#18) and Unix socket / named pipe listeners (#19).
- tokio is already a workspace dependency.
- smol stays where it is today: in the desktop and `waku-client`. The desktop does not depend on `waku-core`.

**Interim before #18 lands.** The `ProviderSessionManager` (#44) creates the runtime itself and bridges to the current daemon through the existing crossbeam channel plus smol wake. Driver ports do not wait for #18.

## Trait

```rust
// crates/waku-core/src/driver/v2/mod.rs
use std::sync::Arc;
use async_trait::async_trait;

pub type DriverResult<T> = Result<T, DriverError>;
pub const ADAPTER_EVENT_CAPACITY: usize = 1024;
pub type AdapterEventSender = tokio::sync::mpsc::Sender<AdapterEvent>;
pub type AdapterEventReceiver = tokio::sync::mpsc::Receiver<AdapterEvent>;

/// One implementation per transport family. Holds no per-session state.
#[async_trait]
pub trait DriverV2: Send + Sync + 'static {
    fn kind(&self) -> DriverKind;

    /// The most this instance can do, before any session exists. Used by the
    /// settings UI and composer affordances. Pure: no I/O, no process spawn.
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
    /// Single consumer. The ProviderSessionManager fans it out.
    pub events: AdapterEventReceiver,
    /// Negotiated after the handshake. Never stronger than
    /// `DriverV2::capabilities`. Persisted on the ProviderSession.
    pub capabilities: Capabilities,
}

#[async_trait]
pub trait DriverSession: Send + Sync {
    fn capabilities(&self) -> &Capabilities;

    async fn ensure_thread(&self, req: EnsureThread) -> DriverResult<ProviderThreadRef>;
    async fn resume_thread(&self, req: ResumeThread) -> DriverResult<ProviderThreadRef>;

    /// Resolves when the provider accepted the input: an RPC ack, or the
    /// echoed user message on stream transports. It does not wait for the turn
    /// to end. The end arrives as exactly one `AdapterEvent::TurnTerminal` for
    /// `req.run`.
    async fn start_turn(&self, req: TurnStart) -> DriverResult<TurnAccepted>;

    /// Called only when `caps.turns.active_steering`. Resolves once the
    /// message is accepted into the running turn. Returns
    /// `Err(SteerRejected)` when the turn has already settled.
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
        Err(DriverError::Unsupported { operation: "apply_selection" })
    }

    // Optional. Each one is gated by a capability.
    async fn compact(&self, _t: &ProviderThreadRef) -> DriverResult<()> {
        Err(DriverError::Unsupported { operation: "compact" })
    }
    async fn inject_history(&self, _h: HistoryInjection) -> DriverResult<()> {
        Err(DriverError::Unsupported { operation: "inject_history" })
    }
    async fn goal(&self, _op: GoalRequest) -> DriverResult<()> {
        Err(DriverError::Unsupported { operation: "goal" })
    }
    async fn stop_background_work(&self, _w: BackgroundWorkRef) -> DriverResult<()> {
        Err(DriverError::Unsupported { operation: "stop_background_work" })
    }
    async fn stop_subagent(&self, _s: SubagentRef) -> DriverResult<SubagentStopOutcome> {
        Err(DriverError::Unsupported { operation: "stop_subagent" })
    }
    async fn unload_thread(&self, _t: &ProviderThreadRef) -> DriverResult<()> {
        Ok(())
    }
    fn has_pending_background_work(&self) -> bool {
        false
    }
    /// Debug-build extension for Computer Use (see `computer_use::resolve_enabled`).
    fn computer_use(&self) -> Option<&dyn ComputerUseControl> {
        None
    }

    /// Graceful shutdown: close stdin, or unsubscribe for adopted services.
    /// Never terminates a service Waku did not start (OpenCode).
    async fn close(&self) -> DriverResult<()>;
}
```

`ComputerUseControl` carries today's `run_computer_tool`, `reject_computer_tool` and `cancel_computer_use` unchanged.

### Session context and supporting types

The entity structs (`ProviderSession`, `ProviderThread`, `ProviderTurn`, `ExecutionNode`, `Subagent`, `Message`, `TurnItem`, `RuntimeRequest`, `Plan`) and the app id newtypes are the projection types from #42 (`waku-orchestrator::model`). Drivers construct them; they do not define them.

```rust
pub enum DriverKind { Codex, Claude, Acp, OpenCode, Pi, Amp, DeepSeek }

/// One configured provider instance (#80). `provider` selects the flavor
/// inside a driver family (Cursor/Grok/Kimi/Fx for Acp, Pi/OhMyPi for Pi).
pub struct InstanceConfig {
    pub instance_id: ProviderInstanceId,
    pub provider: ProviderKind,
    pub binary: PathBuf,
    pub env: Vec<(OsString, OsString)>,
    /// CODEX_HOME, CLAUDE_CONFIG_DIR, Pi's agent dir, … `None` = the CLI default.
    pub provider_home: Option<PathBuf>,
    /// Probed at instance start. Capabilities may depend on it.
    pub cli_version: Option<semver::Version>,
    pub computer_use_enabled: bool,
    /// Driver-specific settings (agent preset default, …), validated by the driver.
    pub extra: serde_json::Value,
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
    /// Thread-scoped app MCP server (#72, #84). Drivers that cannot inject it
    /// must still start without it.
    pub mcp: Option<McpInjection>,
    /// Command output hub (RFC 0003). Never persisted.
    pub output: OutputSink,
    /// Rotating raw-frame log (RFC 0001: raw frames never go to SQLite).
    pub raw_log: RawFrameLog,
    pub shutdown: tokio_util::sync::CancellationToken,
}

pub struct ResumeState {
    pub session: ProviderSession,
    pub threads: Vec<ProviderThreadRef>,
}

pub struct ProviderThreadRef {
    pub provider_thread_id: ProviderThreadId,   // app id
    pub native: NativeRef,
    /// The existing `ProviderResumeCursor` enum, kept as the per-driver opaque
    /// cursor (session file, resume_at, directory, …). Persisted in the
    /// ProviderThread payload.
    pub cursor: ProviderResumeCursor,
}

pub struct ProviderTurnRef {
    pub provider_turn_id: ProviderTurnId,        // app id
    pub native: NativeRef,
    pub ordinal: u32,                            // 1-based within the provider thread
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativeRef { pub id: String, pub strength: RefStrength }

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RefStrength {
    /// Stable and unique: the provider addresses the entity by it.
    Strong,
    /// Usable for correlation inside a session, but not across restarts or forks.
    Weak,
    /// No native id; the driver keys by ordinal.
    #[default]
    None,
}

pub struct RunRef {
    pub thread_id: ThreadId,
    pub run_id: RunId,
    pub attempt_id: RunAttemptId,
    pub root_node_id: NodeId,
}

/// User-chosen settings. Today's `SessionOptions` plus agent preset and
/// interaction mode.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selection {
    pub model: Option<String>,               // never the sentinel "default"
    pub reasoning_effort: Option<String>,
    pub service_tier: Option<String>,
    pub context_window: Option<String>,
    pub agent_preset: Option<String>,
    pub runtime_mode: RuntimeMode,
    pub interaction_mode: InteractionMode,   // Default | Plan
}

pub enum TransitionPlan {
    /// Per-turn parameters; the next `start_turn` carries the change.
    ApplyOnNextTurn,
    /// Call `apply_selection` on the live session, then continue.
    ApplyInSession,
    /// Close the session and reopen it on the same native thread (today's
    /// `apply_options == false`).
    RestartSession,
    /// The native thread cannot carry the change. Start a new provider thread
    /// with a context handoff.
    CreateWithHandoff,
    Reject { reason: String },
}

pub struct EnsureThread {
    pub thread_id: ThreadId,
    pub title: Option<String>,
    pub selection: Selection,
    pub existing: Option<ProviderThreadRef>,
}

pub struct ResumeThread { pub thread: ProviderThreadRef, pub selection: Selection }

pub struct TurnStart {
    pub run: RunRef,
    pub run_ordinal: u32,
    pub provider_turn_ordinal: u32,
    /// Set on a steering-restart or retry attempt.
    pub restart_continuation_of: Option<RunId>,
    pub thread: ProviderThreadRef,
    pub message: TurnMessage,
    pub selection: Selection,
    /// Handoff, fork context and app instructions. The driver picks the
    /// channel by kind and its `context` capabilities: developer instructions,
    /// a system prompt flag, or a synthetic user envelope.
    pub app_context: Vec<ContextBlock>,
    pub intent: TurnIntent,
}

pub enum TurnIntent { Prompt, SlashCommand { name: String }, Goal(GoalRequest) }
pub struct TurnMessage { pub message_id: MessageId, pub text: String, pub attachments: Vec<Attachment> }
pub struct Attachment { pub blob: BlobId, pub name: String, pub mime: String, pub bytes: u64, pub local_path: Option<PathBuf> }
pub struct ContextBlock { pub key: String, pub kind: ContextKind, pub text: String }
pub enum ContextKind { System, Developer, SyntheticUser }
pub struct TurnAccepted { pub provider_turn: Option<ProviderTurnRef> }

pub struct Steer { pub run: RunRef, pub thread: ProviderThreadRef, pub provider_turn: Option<ProviderTurnRef>, pub message: TurnMessage }
pub struct Interrupt { pub run: RunRef, pub thread: ProviderThreadRef, pub provider_turn: Option<ProviderTurnRef>, pub restart_follows: bool }

pub struct RuntimeResponse { pub request_id: RuntimeRequestId, pub native: NativeRef, pub reply: RuntimeReply }
pub enum RuntimeReply {
    /// `option_id` is the provider's own option id, exactly as it was emitted.
    Decision { option_id: String },
    Answers(Vec<UserInputAnswer>),
    Dismiss,
}

pub struct ThreadSnapshot {
    pub thread: ProviderThread,
    pub turns: Vec<ProviderTurn>,
    pub messages: Vec<Message>,
    pub pending_requests: Vec<RuntimeRequest>,
}

pub struct Rollback {
    pub thread: ProviderThreadRef,
    pub target: RollbackTarget,
    /// Provider turns in order, so drivers without native turn ids can count.
    pub turns: Vec<ProviderTurnRef>,
    pub turns_to_remove: u32,
}
pub enum RollbackTarget { ThreadStart, AfterProviderTurn(ProviderTurnRef) }
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
pub enum ForkPoint {
    Tip,
    AfterProviderTurn { turn: ProviderTurnRef, turns_to_remove: u32 },
    SubagentThread(NativeRef),
}

/// Native ids map to app ids deterministically: UUIDv5 over
/// (scope app id, "<kind>:<native id>"). This needs no storage, gives the same
/// result on replay and survives restarts, because scope ids are persisted.
/// `fresh` is UUIDv7 in production and a counter in tests.
pub trait IdAllocator: Send + Sync {
    fn bind(&self, scope: Uuid, kind: EntityKind, native: &str) -> Uuid;
    fn bind_ordinal(&self, scope: Uuid, kind: EntityKind, ordinal: u64) -> Uuid;
    fn fresh(&self, kind: EntityKind) -> Uuid;
}
pub trait Clock: Send + Sync { fn now(&self) -> chrono::DateTime<chrono::Utc>; }
```

Scopes: the provider session id scopes thread bindings. The provider thread id scopes turn, item, request, subagent and plan bindings. When `RefStrength::None`, drivers call `bind_ordinal` with the turn ordinal, or with `turn_ordinal << 20 | item_index` for items.

### Transports

Driver code never calls `Command::spawn`, `TcpStream::connect` or `dirs::home_dir()` directly. It goes through `ctx.transports`, so replay can replace exactly this layer:

```rust
#[async_trait]
pub trait TransportFactory: Send + Sync {
    /// Stdio child process. `channel` names it in replay ("main", "fork-1", …).
    async fn spawn_stdio(&self, channel: &str, spec: ProcessSpec) -> io::Result<StdioTransport>;
    /// HTTP client plus streaming downlinks (OpenCode service, DeepSeek `dsh web`).
    async fn http(&self, channel: &str, base: Url) -> io::Result<Arc<dyn HttpTransport>>;
    /// Root for provider state on disk (Claude projects, Pi session files,
    /// Grok sessions). Replay points it at a temp copy of the fixture's home.
    fn provider_home(&self, cfg: &InstanceConfig) -> PathBuf;
}

pub struct ProcessSpec { pub program: PathBuf, pub args: Vec<OsString>, pub env: Vec<(OsString, OsString)>, pub cwd: PathBuf }

pub struct StdioTransport {
    pub stdin: Box<dyn AsyncWrite + Send + Unpin>,
    pub stdout: Box<dyn AsyncRead + Send + Unpin>,
    pub stderr: Box<dyn AsyncRead + Send + Unpin>,
    pub exit: BoxFuture<'static, io::Result<ExitStatus>>,
    pub pid: Option<u32>,
}

#[async_trait]
pub trait HttpTransport: Send + Sync {
    async fn request(&self, method: Method, path: &str, body: Option<serde_json::Value>) -> io::Result<HttpResponse>;
    async fn stream(&self, path: &str) -> io::Result<BoxStream<'static, io::Result<serde_json::Value>>>;
}
```

The ACP SDK takes the `stdin`/`stdout` pair as its byte streams, so replay covers it with no SDK changes.

## Errors

```rust
#[derive(Debug, thiserror::Error)]
pub enum DriverError {
    #[error("{operation} is not supported by this provider")]
    Unsupported { operation: &'static str },
    #[error("session not ready: {0:?}")]
    NotReady(NotReadyReason),
    #[error("provider session lost")]
    SessionLost { detail: String },
    #[error("steer rejected")]
    SteerRejected { reason: SteerRejectReason },
    #[error("provider failure ({:?})", .0.class)]
    Provider(ProviderFailure),
    #[error("protocol error ({reason:?})")]
    Protocol { reason: ProtocolReason, detail: String },
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub enum NotReadyReason { Connecting, PreviousTurnActive, NoActiveTurn, ThreadMissing }
pub enum SteerRejectReason { TurnSettled, NoActiveTurn, ProviderRefused }
/// Only these fixed reasons reach user-facing text. `detail` can contain
/// private configuration and goes to the raw log only (as in T3).
pub enum ProtocolReason { PreviousTurnActive, AttachmentUnreadable, UnexpectedFrame, Timeout, Decode }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProviderFailure {
    pub class: FailureClass,
    /// Sanitized, user-safe message.
    pub message: String,
    pub retryable: bool,
    /// Set for `UsageLimit` when the provider reports when the window reopens.
    pub reset_at: Option<chrono::DateTime<chrono::Utc>>,
    pub native_code: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass { UsageLimit, ProviderError, TransportError, PermissionError, ValidationError, Unknown }
```

Rules:
- `SessionLost` means the native runtime is gone: process exit, a stream that will not resume, or an expired pooled server. The manager reopens with `ResumeState`.
- OpenCode's stream blink is not `SessionLost`. Reconnection is the service's job.
- Every driver classifies `UsageLimit` and sets `reset_at` whenever the provider supplies it. Unclassified errors are `Unknown`, never `ProviderError`.

## Normalized events

```rust
pub enum AdapterEvent {
    SessionUpdated(ProviderSession),            // status, commands, model, agent preset, last error
    ProviderThreadUpdated { thread: ProviderThread, retained_native_turn_ids: Option<Vec<String>> },
    ProviderTurnUpdated(ProviderTurn),          // status, origin, token/context usage
    NodeUpdated(ExecutionNode),
    SubagentUpdated(Subagent),
    MessageUpdated(Message),
    TurnItemUpdated(TurnItem),
    /// Never persisted. The manager routes it to the output hub (RFC 0003).
    CommandOutputDelta { item: TurnItemId, chunk: bytes::Bytes, replace: bool },
    RuntimeRequestUpdated(RuntimeRequest),      // approval | user_input | dynamic_tool_call | auth_refresh
    PlanUpdated(Plan),                          // todo_list | proposed_plan
    // Waku additions. They have no T3 counterpart in the adapter stream.
    BackgroundWorkUpdated(BackgroundWorkItem),  // processes and monitors that outlive a turn
    GoalUpdated(Option<ThreadGoal>),
    AccountUsageUpdated(PlanUsage),             // instance-level rate-limit meters
    TurnTerminal(TurnTerminal),
}

pub struct TurnTerminal {
    pub run: RunRef,
    pub provider_turn: Option<ProviderTurnRef>,
    pub outcome: TurnOutcome,
    pub thread_disposition: ThreadDisposition,
    /// Subagent turns started by this turn keep running.
    pub child_turns_continue: bool,
    /// Detached work will wake the session later (today's `TurnParked`).
    pub background_pending: bool,
    pub summary: Option<String>,
}

pub enum TurnOutcome {
    Completed,
    /// The user interrupted and the provider honoured it.
    Interrupted,
    /// The turn never ran or was superseded (a dropped queued input, an
    /// interrupt-restart attempt).
    Cancelled,
    Failed(ProviderFailure),
}

pub enum ThreadDisposition { Reusable, Broken }
```

Rules:
- **Entity snapshots.** Events are entity snapshots keyed by app ids. Drivers resolve native ids through `ctx.ids`, and replaying the same frame finds the same binding.
- **Ordering.** `TurnTerminal` is authoritative. Status-idle notifications never complete a run (OpenCode's `session.idle`, ACP status updates).
- **Exactly one terminal.** Every `start_turn` that resolved `Ok` produces exactly one `TurnTerminal`. That holds on completion, provider error, interrupt, process exit and stream end. A turn interrupted before the provider registered it still gets `Cancelled`.
- **Child turns.** A subagent turn's terminal never completes the parent.
- **Self-started turns.** A turn the provider starts on its own (background wake, `/goal` continuation) is reported as `ProviderTurnUpdated { origin: ProviderInitiated, run: None, .. }`. The orchestrator decides which run owns it (RFC 0001 recovery and continuation).
- **Option ids.** Native option ids for permissions and questions survive normalization. Labels are not valid replies. Where a provider has no option ids, the driver uses the constants in its section below.
- **Unknown frames** go to `raw_log` and are otherwise ignored. A port must not fail on an unknown notification.

### `DriverEvent` → `AdapterEvent`

This table is what a port follows. It is also what the `V1Bridge` (D-9) inverts.

| `DriverEvent` today | `AdapterEvent` |
|---|---|
| `Connected { provider_cursor }` | `ProviderThreadUpdated` (native id, cursor) and `SessionUpdated { status: Ready }` |
| `AgentPresetSelected` | `SessionUpdated` (selection.agent_preset) |
| `AutoTitleUpdated` | `ProviderThreadUpdated` (title) |
| `AvailableCommands` | `SessionUpdated` (commands) |
| `PromptSubmitted` | Removed. The orchestrator creates user messages from commands. |
| `TurnStarted` | `ProviderTurnUpdated { status: Running }` and `NodeUpdated` (root `Running`) |
| `TurnParked` | `TurnTerminal { outcome: Completed, background_pending: true }` |
| `TextDelta` | `MessageUpdated` and `TurnItemUpdated` (assistant_message), coalesced |
| `ReasoningDelta` | `TurnItemUpdated` (reasoning) and `NodeUpdated` (reasoning), coalesced |
| `Activity`, `RichActivity` | `TurnItemUpdated` (tool variant from `activity.rs`) and `NodeUpdated` (tool_call) |
| `BackgroundWork` (Process, Monitor) | `BackgroundWorkUpdated` |
| `BackgroundWork` (Subagent) | `SubagentUpdated` |
| `Permission` | `RuntimeRequestUpdated { kind: Approval }` |
| `UserInputRequested` | `RuntimeRequestUpdated { kind: UserInput }` |
| `ComputerUseUpdated` | Stays on `ComputerUseControl` (driver extension) |
| `SteerAccepted` / `SteerRejected` | The result of `steer_turn`, plus `MessageUpdated` for the steering message |
| `UsageUpdated` | `ProviderTurnUpdated` (context tokens, context window) |
| `PlanUsageUpdated` | `AccountUsageUpdated` |
| `GoalUpdated` | `GoalUpdated` |
| `TurnFinished { success, summary }` | `TurnTerminal` |
| `Error(String)` | During a turn: `TurnTerminal { Failed(..) }`. Otherwise: `SessionUpdated { status: Error }` |
| `ProcessExited` | `SessionUpdated { status: Stopped }`, a `TurnTerminal { Failed(TransportError) }` for any open turn, then the stream ends |
| `RuntimeEventCursorAdvanced` | Client-only. Not a driver event. |

## Capabilities

**Decision (D-4).** The groups and field names match T3's `OrchestrationV2ProviderCapabilities` one to one (snake_case), so the T3 matrix maps mechanically. A 13th group, `extras`, holds Waku-only switches. `Default` is the weakest claim: every `bool` is false, every strength is `None` and enforcement is `ClientBoundary`. `#[serde(default)]` on every group means rows persisted before a field existed decode to "not supported". Replay never over-claims.

```rust
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Capabilities {
    pub sessions: SessionCaps,
    pub threads: ThreadCaps,
    pub turns: TurnCaps,
    pub streaming: StreamingCaps,
    pub tools: ToolCaps,
    pub approvals: ApprovalCaps,
    pub planning: PlanningCaps,
    pub subagents: SubagentCaps,
    pub context: ContextCaps,
    pub checkpointing: CheckpointCaps,
    pub identity: IdentityCaps,
    pub runtime_policy: RuntimePolicyCaps,
    pub extras: ExtraCaps,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SessionCaps {
    pub multiple_threads_per_session: bool,
    pub model_switch_in_session: bool,
    pub provider_switching_via_handoff: bool,
    pub runtime_mode_switch_in_session: bool,
    pub pending_requests_survive_restart: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ThreadCaps {
    pub create_empty_thread: bool,
    pub read_snapshot: bool,
    pub rollback: bool,
    pub fork: bool,
    pub fork_from_turn: bool,
    pub fork_from_subagent_thread: bool,
    pub native_thread_id: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TurnCaps {
    pub native_turn_id: bool,
    pub emits_turn_started: bool,
    pub emits_turn_completed: bool,
    pub interrupt: bool,
    pub active_steering: bool,
    /// Native steering cancels pending tools before consuming the message.
    pub active_steering_interrupts_tools: bool,
    pub steering_by_interrupt_restart: bool,
    pub queued_messages: bool,
    pub terminal_status_quality: Quality,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct StreamingCaps {
    pub assistant_text: bool,
    pub reasoning: bool,
    pub tool_output: bool,
    pub plan_text: bool,
    pub emits_message_completed: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ToolCaps {
    pub tool_item_ids: bool,
    pub emits_tool_started: bool,
    pub emits_tool_completed: bool,
    pub emits_tool_output: bool,
    pub mcp_tools: bool,
    pub dynamic_tool_callbacks: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ApprovalCaps {
    pub command: bool,
    pub file_read: bool,
    pub file_change: bool,
    pub apply_patch: bool,
    pub native_request_ids: bool,
    /// The reply must reach the same live process. A restart loses the request.
    pub callbacks_are_live_only: bool,
    pub from_subagents: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PlanningCaps {
    pub emits_plan_updated: bool,
    pub emits_todo_list: bool,
    pub emits_proposed_plan: bool,
    pub structured_questions: bool,
    pub plan_deltas_have_item_ids: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SubagentCaps {
    pub supported: bool,
    pub exposes_thread_ids: bool,
    pub emits_lifecycle: bool,
    pub can_wait: bool,
    pub can_close: bool,
    pub can_fork_thread: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ContextCaps {
    pub accepts_system_context: bool,
    pub accepts_developer_context: bool,
    pub accepts_synthetic_user_context: bool,
    pub can_generate_summaries: bool,
    pub can_consume_handoff_summaries: bool,
    pub delta_handoff: bool,
    pub full_thread_handoff: bool,
    /// `None` = the orchestrator default (`DEFAULT_HANDOFF_CHARS = 100_000`).
    pub max_recommended_handoff_chars: Option<u32>,
    /// Waku name for T3's optional `injectHistory`.
    pub native_history_injection: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CheckpointCaps {
    pub app_can_checkpoint_filesystem: bool,
    pub nested_scopes: bool,
    pub provider_can_rollback_conversation: bool,
    pub provider_rollback_returns_snapshot: bool,
    pub provider_can_read_conversation_snapshot: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct IdentityCaps {
    pub thread_ids: RefStrength,
    pub turn_ids: RefStrength,
    pub item_ids: RefStrength,
    pub request_ids: RefStrength,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RuntimePolicyCaps {
    pub enforcement: Enforcement,
    /// Modes the provider enforces itself. A stored mode outside this set runs
    /// as `RuntimeMode::Ask` (Supervised), as in T3's `RuntimePolicy.ts`.
    pub supported_modes: Vec<RuntimeMode>,
    pub plan_mode_toggle: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Enforcement { Native, #[default] ClientBoundary }

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Quality { Strong, Weak, #[default] None }

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ExtraCaps {
    pub native_image_input: bool,          // #78; otherwise the path-mention fallback
    pub image_mime_types: Vec<String>,
    pub goals: bool,                       // #76
    pub compaction: bool,                  // #76; gates `compact`
    pub background_work: bool,             // gates `stop_background_work`
    pub account_usage_stream: bool,        // emits AccountUsageUpdated
    pub agent_presets: bool,
    pub computer_use: bool,                // debug builds only
    pub model_discovery: bool,             // replaces ProviderKind::supports_model_discovery
}
```

`ProviderKind::supports_conversation_rollback`, `supports_conversation_fork` and `supports_model_discovery` are deleted once the desktop reads capabilities from the provider session. Until then, `V1Bridge` answers them from `DriverV2::capabilities`.

### Matrix for the first port

These are the values each port declares. They describe what the ported driver actually does, not what the CLI could do. `Y` = true, `N` = false. `P` means negotiated at the handshake, as the note explains. `→#nn` names the issue that may flip the value later. A port that implements more may declare more, but only with a replay fixture that proves it.

Driver columns: Codex (`codex.rs`), Claude (`claude.rs`), ACP (`acp.rs`, flavors below), OpenCode (`opencode.rs`), Pi (`pi.rs`, Pi and Oh My Pi), Amp (`amp.rs`), DeepSeek (`deepseek.rs`).

| Field | Codex | Claude | ACP | OpenCode | Pi | Amp | DeepSeek |
|---|---|---|---|---|---|---|---|
| **sessions** | | | | | | | |
| multiple_threads_per_session | N | N | N | N | N | N | N |
| model_switch_in_session | Y | Y | P¹ | Y | Y | N | Y |
| provider_switching_via_handoff | Y | Y | Y | Y | Y | Y | Y |
| runtime_mode_switch_in_session | N | N →#47 | N | Y | N | N | Y |
| pending_requests_survive_restart | N | N | N | Y | N | N | N |
| **threads** | | | | | | | |
| create_empty_thread | Y | Y | Y | Y | Y | Y | Y |
| read_snapshot | Y | Y | P² | Y | Y | Y | Y |
| rollback | Y | Y | Grok only | Y | Y | N | Y |
| fork | Y | Y | Grok only | Y | Y | N | Y |
| fork_from_turn | Y | Y | Grok only | Y | Y | N | Y |
| fork_from_subagent_thread | N | N | N | N | N | N | N |
| native_thread_id | Y | Y | Y | Y | Y | Y | Y |
| **turns** | | | | | | | |
| native_turn_id | Y | N | N | N | N | N | Y |
| emits_turn_started | Y | Y | N | Y | Y | N | Y |
| emits_turn_completed | Y | Y | Y | Y | Y | Y | Y |
| interrupt | Y | Y | Y | Y | Y | Y³ | Y |
| active_steering | Y | Y | N | Y | Y | Y | Y |
| active_steering_interrupts_tools | N | Y⁴ | N | N | N | N | N |
| steering_by_interrupt_restart | Y | Y | Y | Y | Y | Y | Y |
| queued_messages | Y | Y | Y | Y | Y | Y | Y |
| terminal_status_quality | Strong | Strong | Strong | Strong | Weak | Weak | Strong |
| **streaming** | | | | | | | |
| assistant_text | Y | Y | Y | Y | Y | Y | Y |
| reasoning | Y | Y | Y | Y | Y | Y | Y |
| tool_output | Y | N | N | Y | N | N | N |
| plan_text | N →#68 | N | N | N | N | N | N |
| emits_message_completed | Y | Y | N | Y | Y | Y | Y |
| **tools** | | | | | | | |
| tool_item_ids | Y | Y | Y | Y | Y | Y | Y |
| emits_tool_started / completed / output | Y | Y | Y | Y | Y | Y | Y |
| mcp_tools | Y | Y | Y | Y | N →#84 | Y | N |
| dynamic_tool_callbacks | N | N | N | N | N | N | N |
| **approvals** | | | | | | | |
| command | Y | Y | Y | Y | N →#50 | N | Y |
| file_read | Y | Y | Y | Y | N | N | N |
| file_change | Y | Y | Y | Y | N →#50 | N | Y |
| apply_patch | Y | N | N | Y | N | N | N |
| native_request_ids | Y | Y | Y | Y | N | N | Y |
| callbacks_are_live_only | Y | Y | Y | N | N | N | Y |
| from_subagents | N →#71 | N | N | N →#71 | N | N | N |
| **planning** | | | | | | | |
| emits_plan_updated / emits_todo_list | Y | Y | Y | N | N | N | Y |
| emits_proposed_plan | N →#68 | N →#68 | N | N | N | N | N |
| structured_questions | Y | Y | Cursor, Grok | Y | N →#50 | N | Y |
| plan_deltas_have_item_ids | N | N | N | N | N | N | N |
| **subagents** | | | | | | | |
| supported / emits_lifecycle | Y | Y | N | N →#71 | N | N | Y |
| exposes_thread_ids | Y | N | N | N | N | N | N |
| can_wait | N | N | N | N | N | N | N |
| can_close | N | Y | N | N | N | N | N |
| can_fork_thread | N | N | N | N | N | N | N |
| **context** | | | | | | | |
| accepts_system_context | N | Y⁵ | N | N | N | N | N |
| accepts_developer_context | Y | N | N | N | N | N | N |
| accepts_synthetic_user_context | Y | Y | Y | Y | Y | Y | Y |
| can_generate_summaries | N | N | N | N | N | N | N |
| can_consume_handoff_summaries, delta_handoff, full_thread_handoff | Y | Y | Y | Y | Y | Y | Y |
| max_recommended_handoff_chars | None | None | None | None | None | None | None |
| native_history_injection | N →#75 | N | N | N | N | N | N |
| **checkpointing** | | | | | | | |
| app_can_checkpoint_filesystem, nested_scopes | Y | Y | Y | Y | Y | Y | Y |
| provider_can_rollback_conversation | = threads.rollback | | | | | | |
| provider_rollback_returns_snapshot | Y | N | N | N | N | N | N |
| provider_can_read_conversation_snapshot | = threads.read_snapshot | | | | | | |
| **identity** | | | | | | | |
| thread_ids | Strong | Strong | Strong | Strong | Strong | Strong | Strong |
| turn_ids | Strong | Weak⁶ | None | Weak⁷ | Weak⁸ | None | Strong⁹ |
| item_ids | Strong | Strong | Weak | Strong | Weak | Weak | Strong |
| request_ids | Strong | Strong | Strong | Strong | None | None | Strong |
| **runtime_policy** | | | | | | | |
| enforcement | Native | Native | Native | Native | ClientBoundary | ClientBoundary | Native |
| supported_modes | all 4 | all 4 | per flavor¹⁰ | all 4 | FullAccess | FullAccess | all 4 |
| plan_mode_toggle | N →#68 | N →#68 | N | Y¹¹ | N | N | N |
| **extras** | | | | | | | |
| native_image_input | N →#78 | N →#78 | N →#78 | N →#78 | N →#78 | N | N →#78 |
| goals | Y | N →#76 | N | N | N | N | N |
| compaction | N →#76 | N →#76 | N | N | N | N | N |
| background_work | Y | Y | N | Y | N | N | Y |
| account_usage_stream | Y | N | N | N | N | N | N |
| agent_presets | N | N | N | Y | N | Y | Y |
| computer_use | Y | N | Grok | Y | Pi only | N | N |
| model_discovery | Y | Y | Y | Y | Y | N | Y |

Notes:
1. True when the agent advertises a `model` config option or `session/set_model`.
2. True when `initialize` returns `agentCapabilities.loadSession`. The snapshot is the `session/load` replay.
3. Amp has no stream interrupt. The driver sends SIGINT to the process (see its section).
4. Taken from T3's Claude adapter. `message_steering` and `turn_interrupt_mid_tool` fixtures must confirm it; if they do not, declare N.
5. Via `--append-system-prompt` at launch. Only blocks present at `open_session` can use it; later blocks go in the synthetic user envelope.
6. The assistant message uuid that ends the turn (today's `resume_at`). Valid for rollback inside one session file.
7. The id of the user message that opens the turn (`opencode_session::native_turn_message_ids`).
8. The session-file entry id of the user message (`get_fork_messages`).
9. The `seq` of the `turn/start` event (`completed_turn_seqs`).
10. From `acp::desired_access_mode`: Cursor, Grok and Kimi map all four. Fx maps Ask and FullAccess only.
11. Through the `plan` agent preset.

### ACP flavors

| Flavor | Launch | Steering | Rollback / fork | Questions | Notes |
|---|---|---|---|---|---|
| Cursor (`cursor-agent acp`) | `acp` | interrupt-restart | N. The orchestrator uses a portable handoff (replaces `cursor_session.rs`). | Y (Cursor extension) | Resume with `session/load`. |
| Grok (`grok agent [--reasoning-effort e] stdio`) | env `GROK_OAUTH2_REFERRER=waku` | interrupt-restart | Y, replaced: `grok_session::fork_session_at_turn` (`session/fork` on a short-lived second server, then on-disk truncation) | Y (`x.ai/ask_user_question`) | Effort is a launch argument. Computer Use in debug builds. |
| Kimi (`kimi acp`) | `acp` | interrupt-restart | N. `session/fork` copies a whole session (tip only); a later port may set `fork = true, fork_from_turn = false`. | N | Turn failures are read from the wire log (`kimi_session::turn_failure`). |
| Fx (`fx acp`) | `acp` | interrupt-restart | N | N | Ask and FullAccess modes only. |

**D-7.** Today the ACP driver steers by sending a second `session/prompt` that supersedes the running one. That was probed for Cursor and Grok, never for Kimi, and it is outside the ACP spec. The port removes it. The orchestrator's `steering_by_interrupt_restart` policy sends `session/cancel`, waits for the `cancelled` stop reason, and then sends `session/prompt` as a new `RunAttempt { steering_restart }`. This matches #48 and T3.

## Selection transitions

`plan_selection_transition` per driver. A row applies when only that field changes. With several changes, the strongest plan wins, in this order: `Reject` > `CreateWithHandoff` > `RestartSession` > `ApplyInSession` > `ApplyOnNextTurn`.

| Change | Codex | Claude | ACP | OpenCode | Pi | Amp | DeepSeek |
|---|---|---|---|---|---|---|---|
| model | ApplyOnNextTurn | ApplyInSession (`set_model`) | ApplyInSession if advertised, else RestartSession | ApplyInSession (`POST …/model`) | ApplyInSession (`set_model`) | RestartSession | ApplyInSession (`session.selectModel`) |
| reasoning_effort | ApplyOnNextTurn | RestartSession (`--effort`) | ApplyInSession (config option); Grok: RestartSession | ApplyOnNextTurn (variant) | ApplyInSession (`set_thinking_level`) | RestartSession | ApplyInSession |
| service_tier | ApplyOnNextTurn | ApplyOnNextTurn (ignored) | ApplyOnNextTurn (ignored) | ApplyOnNextTurn (ignored) | ApplyOnNextTurn (ignored) | RestartSession (`--fast`) | ApplyOnNextTurn (ignored) |
| context_window | ApplyOnNextTurn | ApplyInSession (`[1m]` model suffix through `set_model`) | ApplyInSession if advertised | ApplyOnNextTurn | ApplyOnNextTurn | RestartSession | ApplyInSession |
| runtime_mode | RestartSession | RestartSession →#47 | RestartSession | ApplyInSession (driver-side policy) | Reject unless FullAccess | Reject unless FullAccess | ApplyInSession |
| agent_preset | ApplyOnNextTurn (ignored) | ApplyOnNextTurn (ignored) | ApplyOnNextTurn (ignored) | ApplyOnNextTurn (agent per prompt) | ApplyOnNextTurn (ignored) | RestartSession (`--mode`) | CreateWithHandoff (preset fixed at `session.create`) |
| interaction_mode | Reject →#68 | Reject →#68 | Reject | ApplyOnNextTurn (`plan` agent) | Reject | Reject | Reject |

## Degradation policies (orchestrator side)

| Missing capability | Policy |
|---|---|
| `turns.active_steering` | Interrupt, then restart within the same run (a new `RunAttempt { steering_restart }`, outcome `Cancelled` on the old attempt) |
| `threads.fork` | Portable context handoff on the fork's first run, sent as `ContextKind::SyntheticUser` |
| `threads.rollback` | Fresh provider thread plus a checkpoint-summary handoff, marked divergent |
| `RollbackOutcome.replaced` | Re-point the app thread at the new `ProviderThread`; the old one is kept for lineage |
| `approvals.*` with `enforcement = ClientBoundary` | Offer only `supported_modes`; anything else runs as Supervised, or is refused when the set is FullAccess only, with a warning |
| `planning.structured_questions` | No question UI; the provider's own question flow runs unanswered |
| `extras.native_image_input` | Path mention fallback |
| `turns.native_turn_id` | Correlate by ordinal; rollback counts provider turns |

## Per-driver mapping

Each table maps every `DriverSession` method to the native call. It names the current code to lift and lists the gaps the port must declare or close. "Replaced" means `RollbackOutcome { replaced: true }`.

### Codex (`driver/codex.rs`, `codex_session.rs`) — #46

| DriverV2 | Native |
|---|---|
| `open_session` | Spawn `codex app-server` (plus `-c` overrides when Computer Use is on); `initialize` → `initialized` → `skills/extraRoots/set` |
| `ensure_thread` | `thread/start { cwd, model, config }` |
| `resume_thread` | `thread/resume { threadId }` |
| `start_turn` | `turn/start { threadId, input: [{type:"text"}], model, effort, serviceTier, approvalPolicy, sandboxPolicy }`; the response carries the turn id. `ContextKind::Developer` blocks go in `collaborationMode.settings.developer_instructions`, as T3's `CodexAdapterV2` does. |
| `steer_turn` | `turn/steer { threadId, expectedTurnId, input }`; the error response → `SteerRejected { TurnSettled }` |
| `interrupt_turn` | `turn/interrupt { threadId, turnId }`. Today Stop also drops the runtime; the port keeps the session (`Reusable`). |
| `respond` (approval) | JSON-RPC result `{ decision }` for any `*requestApproval` request. Native option ids: `accept`, `acceptForSession`, `decline`. |
| `respond` (question) | Result `{ answers: { <questionId>: { answers: [..] } } }` for `item/tool/requestUserInput` |
| `read_snapshot` | `thread/read` on the live connection (today it runs on a short-lived app-server, `codex_session.rs:382`) |
| `rollback` | `thread/rollback { threadId, numTurns }`, in place. The returned thread → `snapshot`. `/goal` control turns are excluded from `numTurns`. |
| `fork` | `thread/fork { threadId, lastTurnId, cwd, excludeTurns: true, deferGoalContinuation: true }` on the live connection (today: `codex_session::fork_session_at_turn` on a second app-server) |
| `goal` | `thread/goal/get`, `thread/goal/set`, `thread/goal/clear`; `thread/goal/updated` / `cleared` → `GoalUpdated` |
| `stop_background_work` | `thread/backgroundTerminals/list`, `thread/backgroundTerminals/terminate` |
| events | `turn/started`, `turn/completed` (terminal); `item/started`, `item/completed`; `item/agentMessage/delta`; `item/reasoning/textDelta`, `summaryTextDelta`; `item/commandExecution/outputDelta` → `CommandOutputDelta`; `thread/tokenUsage/updated` → `ProviderTurnUpdated`; `account/rateLimits/updated` → `AccountUsageUpdated`; `thread/name/updated` → `ProviderThreadUpdated`; `collabAgentToolCall` items → `SubagentUpdated`; `turn/plan/updated` → `PlanUpdated` (new); `mcpServer/startupStatus/updated` → `SessionUpdated` |

Gaps (declared N): `mcpServer/elicitation/request`, `thread/compact/start` (#76), `thread/inject_items` (#75), `localImage` input (#78), async questions answered by message, MCP Apps, `feedback/upload` (#83), approvals from subagent threads (#71), `<proposed_plan>` (#68).

### Claude (`driver/claude.rs`, `claude_session.rs`) — #47

| DriverV2 | Native |
|---|---|
| `open_session` | Spawn `claude -p --input-format stream-json --output-format stream-json --verbose --include-partial-messages --thinking-display summarized --replay-user-messages --permission-prompt-tool stdio --permission-mode <default\|acceptEdits\|auto\|bypassPermissions> [--dangerously-skip-permissions] [--model m] [--effort e] (--resume <id> \| --session-id <uuid>)` |
| `ensure_thread` | Choose a UUID and pass it as `--session-id`. No I/O; the session file appears with the first message. |
| `resume_thread` | `--resume <id>` at launch. On a live process with a different id: close and reopen. |
| `start_turn` | Write `{"type":"user","message":{"role":"user","content":[{"type":"text",…}]},"parent_tool_use_id":null}`; the turn starts at the echoed user message |
| `steer_turn` | The same write while a turn is active. It is folded in at the next model call. |
| `interrupt_turn` | `{"type":"control_request","request_id":"waku-<n>","request":{"subtype":"interrupt"}}` |
| `respond` (approval) | `control_response` to `can_use_tool`: `{behavior:"allow"}` or `{behavior:"deny", message}`. There are no native options; the option ids are the constants `allow` and `deny`. |
| `respond` (question) | The `can_use_tool` request for tool `AskUserQuestion`, answered with `updatedInput` carrying the answers |
| `read_snapshot` | Parse `<CLAUDE_CONFIG_DIR or ~/.claude>/projects/**/<session>.jsonl` (`claude_session::provider_session_history`) under `transports.provider_home` |
| `rollback`, `fork` | Replaced: `claude_session::fork_session_at` (a JSONL copy truncated at the turn's assistant uuid, with a new session id and `resume_at`). The target is native `--resume <id> --fork-session --resume-session-at <uuid>`; see "Claude transport". |
| `apply_selection` | `control_request { subtype: "set_model", model }` (the model id carries the `[1m]` window) |
| `stop_background_work`, `stop_subagent` | `control_request { subtype: "stop_task", task_id }` |
| events | `system/init` → `SessionUpdated` (commands, model); `stream_event` `content_block_delta` (`text_delta`, `thinking_delta`) → coalesced snapshots; `assistant` (`tool_use`) → tool items; `user` (`tool_result`) → tool completion; `system` `task_started`, `task_progress`, `task_notification`, `task_updated`, `background_tasks_changed` → `SubagentUpdated` / `BackgroundWorkUpdated`; `TodoWrite` → `PlanUpdated`; `result` → `TurnTerminal` (`is_error` → `Failed`); `rate_limit_event` → `UsageLimit` with `reset_at` (new) |

Gaps: native turn ids, `ExitPlanMode` and plan mode (#68), `set_permission_mode` (#47), image content blocks (#78), `/compact` and `compact_boundary` (#76), goals through the Stop hook (#76), `--mcp-config` injection (#84), subagent transcripts (#71).

### ACP family (`driver/acp.rs`, `acp_session.rs`, `grok_session.rs`, `kimi_session.rs`) — #48

| DriverV2 | Native (agent-client-protocol 2.0 SDK) |
|---|---|
| `open_session` | Spawn per flavor (table above) on the dedicated ACP thread; `InitializeRequest` with fs read/write capabilities |
| `ensure_thread` | `NewSessionRequest { cwd, mcpServers }` |
| `resume_thread` | `ResumeSessionRequest` if advertised, else `LoadSessionRequest` (session updates suppressed during the load replay) |
| `start_turn` | `PromptRequest { sessionId, prompt: [ContentBlock::Text] }`; `PromptResponse.stopReason` → `TurnTerminal` (`end_turn` → Completed, `cancelled` → Interrupted, `refusal` / `max_tokens` → Failed) |
| `steer_turn` | `Unsupported` (D-7) |
| `interrupt_turn` | `CancelNotification` |
| `respond` (approval) | `RequestPermissionResponse { outcome: Selected { optionId } }` with the agent's own option ids |
| `respond` (question) | Cursor's question extension; Grok's `x.ai/ask_user_question` response |
| `read_snapshot` | `session/load` replay (`acp_session::provider_session_history`) |
| `rollback`, `fork` | Grok only, replaced (see flavors). Others: `Unsupported`. |
| `apply_selection` | `SetSessionConfigOptionRequest` (model, thought level, …), `SetSessionModeRequest` |
| events | `session/update`: `agent_message_chunk`, `agent_thought_chunk`, `tool_call`, `tool_call_update`, `plan` → `PlanUpdated`, `available_commands_update`, `current_mode_update`, `usage_update` |

Gaps: `ContentBlock::Image` when `promptCapabilities.image` (#78; force it on for Grok, as T3 does), elicitation, Kimi tip fork, Grok `x.ai/task_*` subagents and background work (#71), usage-limit classification from error payloads.

### OpenCode (`driver/opencode.rs`, `opencode_api.rs`, `opencode_service.rs`, `opencode_session.rs`) — #49

| DriverV2 | Native (HTTP + SSE against the user's adopted service) |
|---|---|
| `open_session` | `opencode_service::attached` / adopt; subscribe the shared hub on `GET /api/event` and filter by `sessionID` |
| `ensure_thread` | `POST /api/session` |
| `resume_thread` | `GET /api/session/{id}`, then re-list pending permissions (`GET /api/session/{id}/permission`) and forms (`GET /api/session/{id}/form`) |
| `start_turn` | `POST /api/session/{id}/prompt`; slash commands use `POST /api/session/{id}/command` |
| `steer_turn` | `POST /api/session/{id}/prompt` with `delivery: "steer"` (an inbox entry that queued anyway is promoted) |
| `interrupt_turn` | `POST /api/session/{id}/interrupt` |
| `respond` (approval) | `POST /api/session/{id}/permission/{rid}/reply` with `once` or `reject`. `always` is stored in the driver and never sent (it would write the global `/api/permission/saved`). |
| `respond` (question) | `POST /api/session/{id}/form/{fid}/reply` |
| `read_snapshot` | `GET /api/session/{id}/message` pages |
| `rollback`, `fork` | Replaced: `POST /api/session/{id}/fork { before: <user message id> }` (`opencode_session::fork_session_removing_turns`). `revert/*` stays unused. |
| `apply_selection` | `POST /api/session/{id}/model`; the runtime mode is the driver's own permission policy |
| events | `session.execution.started` / `succeeded` / `failed` / `interrupted` (the terminal; never wait for `session.idle`); `session.text.*`, `session.reasoning.*`; `session.tool.*` (`progress` → `CommandOutputDelta`); `permission.asked` / `replied`; form events; `session.inbox.*`; `session.shell.*` → `BackgroundWorkUpdated`; `session.usage.recorded`; `session.retry.scheduled`; `session.compaction.*` |

Gaps: child-session subagents (#71), todos, file parts for images (#78), per-thread MCP entries through `/api/experimental/mcp` (#84).

### Pi and Oh My Pi (`driver/pi.rs`, `pi_session.rs`) — #50

| DriverV2 | Native (NDJSON RPC) |
|---|---|
| `open_session` | Spawn `pi --mode rpc --approve` or `omp --mode rpc --yolo` (plus `--extension` and `--skill` for Computer Use); `negotiate_protocol` (Oh My Pi v2 chunking, `rpc_chunk`); `get_state` |
| `ensure_thread` | Implicit: the process creates a session; `get_state` reports the id and session file |
| `resume_thread` | `switch_session` to the cursor's session file |
| `start_turn` | `{"type":"prompt", …}` |
| `steer_turn` | `{"type":"steer", …}` |
| `interrupt_turn` | `{"type":"abort"}` |
| `respond` | None today. Pi's `extension_ui_request` (`select`, `confirm`, `input`, `editor`) is auto-cancelled with `extension_ui_response { cancelled: true }`. |
| `read_snapshot` | Parse the session file (`pi_session::provider_session_history`) |
| `rollback` | Replaced: `get_fork_messages`, then `fork` (Pi) or `branch` (Oh My Pi) at the first removed user entry, then `get_state` for the new cursor |
| `fork` | As rollback, followed by `switch_session` back to the source. A tip fork on Oh My Pi uses `omp --fork` out of process (channel `fork-1`). |
| `apply_selection` | `set_model`, `set_thinking_level` |
| events | `agent_start`, `turn_start`, `message_update` (`text_delta`, `thinking_delta`), `message_end`, `tool_execution_start` / `end`, `agent_settled` (Pi) or `agent_end` (Oh My Pi) → terminal, `session_info_changed`, `auto_retry_end`, `get_session_stats` → usage, `available_commands_update` |

Gaps: approvals through a tool hook, and stop forcing `--yolo` on Oh My Pi (#50; then `enforcement = Native` and `supported_modes` grows); extension dialogs as structured questions (#50); `images` on `prompt` (#78); `compact` (#76); MCP through a generated extension (#84).

### Amp (`driver/amp.rs`, `amp_session.rs`) — #51

| DriverV2 | Native (stream-json over stdio) |
|---|---|
| `open_session` | Spawn `amp [threads continue <T-id>] --execute --stream-json-thinking --stream-json-input --dangerously-allow-all [--mode m] [--effort e] [--fast]` |
| `ensure_thread` | `amp threads new` (`amp_session::create_thread`), then open with `threads continue` |
| `resume_thread` | `threads continue <id>` at launch |
| `start_turn` | A user message line on stdin (synthetic context wrapped per port step 11) |
| `steer_turn` | A user message line with top-level `"steer": true` |
| `interrupt_turn` | SIGINT to the process (`active_pid`). Emit `TurnTerminal { Interrupted, Reusable }`; the manager reopens with `threads continue` on the next turn. |
| `respond` | None |
| `read_snapshot` | `amp threads export <id>` |
| `rollback`, `fork` | `Unsupported` (D-6). The export, new thread and `WAKU_AMP_BRANCH_CONTEXT_V1` envelope become the orchestrator's portable handoff. The driver still wraps `ContextKind::SyntheticUser` blocks in `start_turn`, using the shared envelope from port step 11. |
| `apply_selection` | `Unsupported`; every change restarts |
| events | `system/init`; `assistant` (`text`, `thinking`, `tool_use`; `stop_reason: "end_turn"` → terminal); `user` (`tool_result`); `result` |

Gaps: approvals (declared as a gap, per #51), interrupt without killing the process, model discovery, usage, a pushed title (it is polled from the `threads` index today).

### DeepSeek Harness (`driver/deepseek.rs`, `deepseek_session.rs`, `deepseek_pool.rs`) — #52

| DriverV2 | Native (`dsh web`: typed HTTP envelopes plus downlink streams) |
|---|---|
| `open_session` | `deepseek_pool::acquire(binary)` (a pooled server), then subscribe the session downlink |
| `ensure_thread` | `session.create { cwd, sessionId, agentPreset }` |
| `resume_thread` | Subscribe, then `session.history` (paged by `beforeSeq`) to rebuild `completed_turn_seqs` |
| `start_turn` | `session.prompt { sessionId, mode: "queue", content: [{type:"text"}] }`; slash commands use `commands/execute { args: { agentId, line, images: [] } }` |
| `steer_turn` | `session.prompt { mode: "steer" }` |
| `interrupt_turn` | `session.cancel { sessionId }` |
| `respond` (approval) | Downlink RPC response `{ sessionId, approvalId, outcome }`. The port passes Harness outcomes through as option ids (`allowed-once`, `rejected`, and any others the request lists) instead of today's `allow` / `reject`. |
| `respond` (question) | Downlink RPC response with the structured answers |
| `read_snapshot` | `session.history` |
| `rollback`, `fork` | Replaced: `session.fork { sessionId, atSeq }`, or `session.create` when no turn is retained |
| `apply_selection` | `session.selectModel`, plus the session option update `apply_session_options` sends |
| events | `session/event` envelopes: `turn/start` (seq), `turn/end` → terminal, `assistant/chunk`, `assistant/message`, `tool/call`, `tool/result`, `approval/requested` / `resolved`, `question/requested` / `resolved`, `todo/write` → `PlanUpdated`, `session/title`, `session/jobs` → `SubagentUpdated` / `BackgroundWorkUpdated`, `session/projection`, `stream/error`, `commands/change` |

Gaps: the `images` argument is unused (#78); agent presets stay a provider-specific option (per #52); file-read approvals have not been observed.

## Claude transport

**Decision (D-3): stay on stream-json and grow the control protocol in Rust.**

- The Agent SDK's `query()` spawns the same `claude` binary with the same flags and speaks the same `control_request` / `control_response` protocol over stdio. Its extra features are messages on that protocol and CLI flags, not a separate capability.
- The SDK is TypeScript only. Adopting it means shipping and supervising a Node sidecar with its own IPC, in a daemon that has no Node today.
- Waku already implements `can_use_tool`, `interrupt`, `set_model` and `stop_task`, and parses the `task_*` system messages that back background tasks and subagents.

What #47 adds, each behind a fixture:

| Feature | Mechanism |
|---|---|
| Native fork and rollback | `--resume <id> --fork-session` (documented in CLI 2.1.285 `--help`) plus `--resume-session-at <assistant uuid>` (passed by the SDK's `resumeSessionAt`, missing from `--help`; probe it before relying on it). Keep `claude_session::fork_session_at` as the fallback while the probe fails. |
| Runtime-mode switch in session | `control_request { subtype: "set_permission_mode", mode }`, which is what the SDK's `setPermissionMode()` sends. Flip `runtime_mode_switch_in_session` only after a fixture proves it. |
| Usage limits | `rate_limit_event` and `result` errors → `FailureClass::UsageLimit` with `reset_at` |
| Plan mode | Intercept `can_use_tool` for `ExitPlanMode` → `PlanUpdated { proposed_plan }` (#68) |
| MCP injection | `--mcp-config <file>` (#84) |
| Goals | A Stop-hook registration in an `initialize` control request (#76) |

Capabilities are derived from `InstanceConfig.cli_version` and the `system/init` handshake, each with a minimum CLI version recorded next to the fixture.

Revisit this decision only if a needed feature is reachable solely through in-process SDK callbacks with no control-protocol message.

T3's "workflows" are an orchestrator feature (#91), not a Claude SDK feature, so they do not affect this choice.

## Replay fixtures

**Decision (D-8).** Use T3's record shapes, so the envelope matches T3 and T3's `decodeProviderReplayNdjson` reads our files. Frames are compatible only where the transport boundary is the same: Codex app-server, Pi RPC and ACP stdio. T3 records Claude and OpenCode at the SDK boundary (`query.open`, `prompt.offer`, `sdk.response`, `sdk.event`). Waku records the raw CLI and HTTP frames, so those T3 transcripts are reference material, not inputs.

### File format

UTF-8 NDJSON with one JSON object per line. Blank lines are ignored. The first line must be the header.

```jsonc
{"type":"transcript_start","provider":"codex","protocol":"codex.app-server","version":"0.156.1","scenario":"simple",
 "metadata":{"recordedBy":"waku-replay-record","recordedAt":"2026-10-20","model":"gpt-6-luna",
             "minCliVersion":"0.150.0","ignore":["/params/clientInfo/version"],"synthetic":false}}
{"type":"expect_outbound","label":"initialize","frame":{"id":1,"method":"initialize","params":{…}}}
{"type":"emit_inbound","label":"initialize","frame":{"id":1,"result":{…}}}
{"type":"emit_inbound","label":"turn/completed","frame":{…},"afterMs":40}
{"type":"expect_outbound","label":"thread/fork","channel":"fork-1","frame":{…}}
{"type":"runtime_exit","status":"success"}
```

| Record | Fields |
|---|---|
| `transcript_start` | `provider` (`ProviderKind::id()`: `codex`, `claude`, `cursor`, `grok`, `kimi`, `fx`, `opencode`, `pi`, `ohmypi`, `amp`, `deepseek`); `protocol` (`codex.app-server`, `claude.stream-json`, `acp.stdio`, `opencode.http-sse`, `pi.rpc-jsonl`, `amp.stream-json`, `deepseek.dsh-web`); `version` (CLI version recorded); `scenario`; `metadata` (free-form; the keys below are reserved) |
| `expect_outbound` | `label?`, `channel?` (default `"main"`), `frame` |
| `emit_inbound` | `label?`, `channel?`, `frame`, `afterMs?` (non-negative integer, default 0) |
| `runtime_exit` | `channel?`, `status` (`success` \| `error` \| `cancelled`), `exitCode?`, `signal?`, `error?` |

Reserved `metadata` keys:
- `ignore`: JSON Pointers removed from both sides before comparing outbound frames.
- `minCliVersion`.
- `synthetic`: true when the file was hand-derived, for example usage-limit payloads taken from a real error log.
- `recorderOnlyKeys`: recorder-only config keys; T3's `recorderThreadConfigKeys` is accepted as an alias.

`channel` is a Waku extension. T3's decoder ignores unknown keys.

**Frame shape per transport:**
- **Stdio JSON lines** (Codex, Claude, ACP, Pi, Amp): `frame` is the parsed line. Oh My Pi `rpc_chunk` frames are recorded after reassembly.
- **Process start:** each spawn records `expect_outbound { frame: { "type": "process_start", "program": "<basename>", "args": […], "env": { only keys the driver sets } } }` on its channel, as T3's Pi transcripts do.
- **HTTP** (OpenCode, DeepSeek):
  - Outbound: `{"type":"http.request","method":"POST","path":"/api/session/<id>/prompt","body":{…}}`.
  - Inbound response: `{"type":"http.response","status":200,"body":{…}}`.
  - Stream items: `{"type":"stream.event","path":"/api/event","data":{…}}`.
  - Stream end: `{"type":"stream.closed","path":"/api/event"}`.

### Matching

1. Outbound frames match the entry at the replay cursor, in order, per transcript (not per channel). Drivers write through one writer per channel, and the actor serializes commands, so order is deterministic for a given inbound order.
2. Before an outbound frame is checked, every `emit_inbound` at the cursor is delivered (T3's `drainInbound`). `afterMs` advances the paused tokio clock, not wall time.
3. Comparison is canonical JSON with sorted keys, after:
   - removing `ignore` pointers and the driver's built-in ignore list (`ReplayProfile::ignored_pointers`, for example the Codex `clientInfo.version`);
   - materializing placeholders.
4. Placeholders are materialized in both directions:
   - `<workspace>` → the harness workspace;
   - `<home>` → the harness provider home;
   - `"<any>"` as a whole value matches anything;
   - a string containing `<any>` is a glob whose `<any>` segments match any substring.
5. JSON-RPC ids are compared literally. Drivers allocate request ids from a per-channel counter starting at 1 (Claude: `waku-<n>`), which is already true today.
6. On a mismatch, the test fails with `ReplayError::Mismatch { scenario, line, label, expected, actual, diff }`. `diff` is a unified diff of the two pretty-printed canonical frames.
7. Other failures:
   - an outbound frame while the cursor is at `runtime_exit` or past the end → `UnexpectedOutbound`;
   - entries left over once the scenario is idle → `NotExhausted { remaining }`;
   - a line that does not parse → `Parse { line }`.
8. On `runtime_exit`, the channel's stdout reaches EOF, or the stream closes, and the process future resolves with that status.

### Sanitization (recorder)

Applied in this order before a file is written:

1. **Paths.** Workspace → `<workspace>`; the provider home (`CODEX_HOME`, `CLAUDE_CONFIG_DIR` or `~/.claude`, Pi's agent dir, …) → `<home>`; any remaining `$HOME` prefix → `/home/replay-user`.
2. **Secret keys.** Any object key matching `(?i)token|secret|password|api[_-]?key|authorization|cookie|bearer|refresh` → `"<redacted>"`.
3. **Secret shapes in strings** → `<redacted>`: `sk-[A-Za-z0-9_-]{20,}`, `gh[pousr]_[A-Za-z0-9]{20,}`, `xox[abpr]-[A-Za-z0-9-]+`, `AKIA[0-9A-Z]{16}`, JWTs (`eyJ[\w-]+\.[\w-]+\.[\w-]+`). Email addresses → `user@example.invalid`. The machine hostname → `replay-host`.
4. **UUIDs.** Renumbered in first-seen order to `00000000-0000-4000-8000-%012d`, consistently across the file and all channels. Other native ids (`T-…`, `ses_…`, `msg_…`) are kept unless the scenario passes `--renumber-ids`.
5. **Installation and telemetry ids** → `00000000-0000-4000-8000-000000000000`.
6. **Timing.** `afterMs` is written only with `--keep-timing`, for interrupt and steer scenarios, as the observed gap rounded to 10 ms. Recorded timestamps inside frames are kept.
7. **Size.** Single string values over 64 KiB are truncated to 64 KiB plus `…<truncated N bytes>`, unless the scenario is about output streaming.
8. **Fail closed.** After writing, the recorder scans the file for:
   - the real `$HOME`;
   - the user name;
   - the value of any environment variable in the deny list (`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`, `XAI_API_KEY`, `DEEPSEEK_API_KEY`, `OPENROUTER_API_KEY`, `GITHUB_TOKEN`, `GH_TOKEN`).

   On any hit, it deletes the file and exits non-zero.

### Locations

```
crates/waku-core/src/driver/replay/          # feature "replay"
  mod.rs          transcript types and the NDJSON reader/writer
  transport.rs    ReplayTransportFactory (stdio + http + provider_home)
  matcher.rs      canonicalize, placeholders, ignore, diff
  sanitize.rs     the rules above
  profile.rs      ReplayProfile per driver (ignored pointers, protocol name)
crates/waku-core/src/bin/waku-replay-record.rs   # feature "replay-recorder"
crates/waku-core/fixtures/replay/
  <scenario>/input.json            # shared steps, provider-neutral
  <scenario>/<provider>.ndjson     # one transcript per provider id
  <scenario>/<provider>.home/      # optional seed for provider_home (Claude JSONL, Pi files, Grok sessions)
crates/waku-orchestrator/tests/replay.rs         # one test per (scenario, provider) file found
crates/waku-orchestrator/tests/replay/<scenario>.rs   # assertions per scenario
```

Scenario names follow T3's, so T3 transcripts can be compared side by side.

### Harness API

```rust
// waku-core::driver::replay
pub struct ReplayTranscript { pub header: TranscriptHeader, pub entries: Vec<ReplayEntry> }
pub fn read_transcript(path: &Path) -> Result<ReplayTranscript, ReplayError>;
pub fn write_transcript(path: &Path, t: &ReplayTranscript) -> io::Result<()>;

pub struct ReplayTransportFactory { /* cursor, channels, workspace, home */ }
impl ReplayTransportFactory {
    pub fn new(t: ReplayTranscript, workspace: PathBuf, home: PathBuf, profile: ReplayProfile) -> Arc<Self>;
    pub fn assert_exhausted(&self) -> Result<(), ReplayError>;
}
impl TransportFactory for ReplayTransportFactory { /* … */ }

pub struct RecordingTransportFactory { /* wraps the real factory, tees frames */ }

// waku-orchestrator::testkit
pub struct ReplayHarness { /* tokio paused runtime, SeqIdAllocator, ManualClock, temp SQLite */ }
impl ReplayHarness {
    pub async fn new(driver: Arc<dyn DriverV2>, fixture: &Path) -> Result<Self, ReplayError>;
    pub async fn run(&mut self, input: &ScenarioInput) -> Result<ScenarioResult, ReplayError>;
}
pub struct ScenarioResult {
    pub projection: ThreadProjection,        // after rebuild-from-log equality is checked
    pub events: Vec<DomainEvent>,
    pub adapter_events: Vec<AdapterEvent>,
    pub checkpoints: Vec<CheckpointRef>,
    pub capabilities: Capabilities,
}
```

The harness runs the real orchestrator, `EventSink`, projections and checkpoint capture (RFC 0001). Only the transport is replaced.
- `ManualClock` starts at `2026-01-01T00:00:00Z` and moves with tokio's paused clock.
- `SeqIdAllocator` hands out `fresh` ids as `00000000-0000-7000-8000-%012d`; `bind` uses the production UUIDv5 rule.
- Every run also checks that rebuilding the projections from the event log equals the live projections.

`input.json`:

```jsonc
{"steps":[
  {"type":"message","text":"Respond with the following text: fixture simple ok"},
  {"type":"wait","until":"tool_started"},                 // tool_started | request_pending | turn_terminal | idle | ms:<n>
  {"type":"steer","text":"…"},
  {"type":"interrupt"},
  {"type":"respond","request":"first_pending","option":"<native option id>"},
  {"type":"answer","request":"first_pending","answers":[{"questionId":"q1","answers":["A"]}]},
  {"type":"rollback","runs":1},
  {"type":"fork","atRun":1,"as":"fork-a"},
  {"type":"select","selection":{"model":"…"}},
  {"type":"restart_daemon"}                               // process-loss recovery; the transcript continues on a new channel
]}
```

### Recorder

```
cargo run -p waku-core --features replay-recorder --bin waku-replay-record -- \
  --provider codex --scenario simple [--binary PATH] [--model M] [--keep-timing] [--renumber-ids]
```

The recorder:
1. Runs `input.json` against the real CLI in a temp workspace, using the real driver over `RecordingTransportFactory`.
2. Writes `<scenario>/<provider>.ndjson`, sanitized.
3. Re-runs the file through `ReplayHarness`.
4. Keeps the file only if replay passes.

The usage-limit scenario may be `synthetic: true`. It is built from a recorded `simple` file with the provider's real limit payload spliced in.

### Required scenarios

R = required. S = required when the capability is declared Y. — = not applicable. Every file must replay green in CI (`cargo test -p waku-orchestrator --features replay`, no network, no CLIs).

| Scenario | Codex | Claude | Cursor | Grok | Kimi | Fx | OpenCode | Pi | Oh My Pi | Amp | DeepSeek |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `simple` | R | R | R | R | R | R | R | R | R | R | R |
| `multi_turn` | R | R | R | R | R | R | R | R | R | R | R |
| `tool_call_read_only` | R | R | R | R | R | R | R | R | R | R | R |
| `tool_call_approval` | S | S | S | S | S | S | S | — | — | — | S |
| `plan_questions` | S | S | S | S | — | — | S | — | — | — | S |
| `turn_interrupt` | R | R | R | R | R | R | R | R | R | R | R |
| `turn_interrupt_mid_tool` | R | R | R | R | R | R | R | R | R | R | R |
| `message_steering` | S | S | — | — | — | — | S | S | S | S | S |
| `steering_restart` | — | — | R | R | R | R | — | — | — | — | — |
| `provider_thread_resume` | R | R | R | R | R | R | R | R | R | R | R |
| `thread_rollback` | S | S | — | S | — | — | S | S | S | — | S |
| `thread_fork_native` | S | S | — | S | — | — | S | S | S | — | S |
| `usage_limit` | R | R | R | R | R | R | R | R | R | R | R |
| `runtime_exit_unexpected` | R | R | R | R | R | R | — | R | R | R | R |
| `background_work` | S | S | — | — | — | — | S | — | — | — | S |
| `subagent` | S | S | — | — | — | — | — | — | — | — | S |

For OpenCode, `runtime_exit_unexpected` is replaced by `stream_resync`: the event stream closes and reopens with no `SessionLost`.

## Port checklist

### Prerequisites

- #44 merged: the trait, `Capabilities`, `ProviderSessionManager`, `V1Bridge`, `DeltaCoalescer`, `IdAllocator`, `TransportFactory`.
- #45 merged: replay, the recorder and `ReplayHarness`.

### Steps for every port

1. Replace `driver/<name>.rs` with `driver/<name>/mod.rs` (the `DriverV2` impl and the session actor), `protocol.rs` (frame types and decode) and `normalize.rs` (native → `AdapterEvent`).
2. Delete the `DriverControl` impl. `start_local` wraps the session in `V1Bridge` until the orchestrator cutover (#165).
3. Use only `ctx.transports` for processes, HTTP and provider files. Move any helper the driver needs from `daemon.rs` (`fork_provider_session`, `rewind_provider_response` arms) and `*_session.rs` into the driver, so `daemon.rs` has no per-provider rollback or fork arm left for it.
4. Bind every native id through `ctx.ids` with the strength from the matrix. When the strength is `None`, use `bind_ordinal`.
5. Declare `Capabilities` as a function of `InstanceConfig` (flavor, CLI version) and of handshake results. Add a snapshot test `capabilities_<provider>.json` that must equal this RFC's matrix row.
6. Implement `plan_selection_transition` exactly as the transitions table says.
7. Map every native frame listed in the driver's section, using the `DriverEvent` → `AdapterEvent` table. Route text and reasoning through `DeltaCoalescer`, tool output through `CommandOutputDelta`, and unknown frames to `raw_log`.
8. Guarantee exactly one `TurnTerminal` per accepted turn on every path. Unit-test the paths: completion, provider error, interrupt before registration, interrupt mid-tool, process exit, stream end.
9. Classify failures. Set `UsageLimit` and `reset_at` from the provider's own fields; never parse a reset time out of free text when a structured field exists.
10. Pass native option ids through unchanged. Use only the constants named in this RFC where the provider has none.
11. Wrap `ContextKind::SyntheticUser` blocks in a provider-neutral envelope (the one `cursor_session` / `amp_session` use today, renamed `WAKU_CONTEXT_V1`), unless the driver declares a native context channel.
12. Record every R and S scenario with `waku-replay-record` and commit them under `fixtures/replay/`.
13. Run a live smoke test against the real CLI and paste the transcript summary in the PR (per the issue's acceptance criteria).
14. Update the provider's column in `docs/providers.md`.

### Per-driver additions

**#46 Codex**
- [ ] Move `thread/fork` and `thread/read` onto the live connection; drop the short-lived app-server in `codex_session.rs` for live sessions (keep it for cold imports).
- [ ] Keep the session on interrupt instead of dropping the runtime.
- [ ] Exclude `/goal` control turns from rollback counts and from `provider_turn_ordinal`.
- [ ] Map `turn/plan/updated` → `PlanUpdated` and `collabAgentToolCall` → `SubagentUpdated` with `exposes_thread_ids`.
- [ ] Classify the usage-limit error code and take `reset_at` from `account/rateLimits/*`.
- [ ] Fixtures: all R; S for approval, plan_questions, steering, rollback, fork, background_work, subagent.

**#47 Claude**
- [ ] Turn ids: `Weak`, bound to the turn-ending assistant uuid; keep `resume_at` in the cursor.
- [ ] Probe `--fork-session` plus `--resume-session-at` against the pinned CLI. If it passes, implement `fork` and `rollback` natively; if not, keep `fork_session_at` (replaced) and record the probe result in the PR.
- [ ] Probe `set_permission_mode`; flip `runtime_mode_switch_in_session` only with a fixture.
- [ ] `rate_limit_event` / `result` errors → `UsageLimit`.
- [ ] Confirm or drop `active_steering_interrupts_tools` with the `message_steering` and `turn_interrupt_mid_tool` fixtures.
- [ ] `stop_subagent` and `stop_background_work` via `stop_task`.
- [ ] Fixtures: all R; S for approval, plan_questions (AskUserQuestion), steering, rollback, fork, background_work, subagent.

**#48 ACP family**
- [ ] One driver; flavor from `InstanceConfig.provider`.
- [ ] Remove the superseding-prompt steer (D-7) and declare `active_steering = false`.
- [ ] Capabilities from the `initialize` response (`loadSession`, session resume and fork support, `promptCapabilities`) and from advertised config options.
- [ ] Grok: move `grok_session::fork_session_at_turn` behind `fork` and `rollback` (replaced, side process on channel `fork-1`, provider files under `provider_home`).
- [ ] Cursor: delete the `fork_context` re-seed path; the orchestrator handoff replaces it (D-6).
- [ ] Kimi and Fx: declare N for rollback, fork and questions.
- [ ] Fixtures per flavor: all R, `steering_restart` instead of `message_steering`, plus S rows for Grok rollback and fork.

**#49 OpenCode**
- [ ] The session never owns the service: `close()` unsubscribes only.
- [ ] Do not map a stream break to `SessionLost`.
- [ ] Re-list pending permissions and forms in `resume_thread` (`pending_requests_survive_restart = true`).
- [ ] Rollback and fork through `POST …/fork { before }`, returning `replaced: true`.
- [ ] `always` approvals stay in driver state, persisted on the `ProviderSession` payload so they survive a reopen.
- [ ] Fixtures: all R except `runtime_exit_unexpected`, which `stream_resync` replaces; S for approval, plan_questions, steering, rollback, fork, background_work.

**#50 Pi / Oh My Pi**
- [ ] One driver; `PiFlavor` from `InstanceConfig.provider`.
- [ ] Rollback and fork via `fork` / `branch`, returning `replaced: true`; the Oh My Pi tip clone runs on channel `fork-1`.
- [ ] Add approvals through a tool hook and stop forcing `--yolo` on Oh My Pi. If that lands in this issue, update the matrix (`approvals.command` and `file_change`, `enforcement = Native`, `supported_modes`) and record `tool_call_approval`. If not, declare FullAccess only.
- [ ] Map `extension_ui_request` (`select`, `confirm`, `input`, `editor`) to `RuntimeRequest { kind: UserInput }` instead of auto-cancelling.
- [ ] Fixtures for both flavors: all R; S for steering, rollback and fork.

**#51 Amp**
- [ ] Interrupt via SIGINT with `TurnTerminal { Interrupted, Reusable }`, then lazy reopen with `threads continue`.
- [ ] Remove the fork and rollback paths from `amp_session.rs` and `daemon.rs` (D-6); keep the envelope wrapper for synthetic context.
- [ ] Declare approvals N, `supported_modes = [FullAccess]` and `enforcement = ClientBoundary`, and document the gap in `docs/providers.md`.
- [ ] Fixtures: all R; S for steering.

**#52 DeepSeek Harness**
- [ ] Pooled server lifetime: `close()` releases the pool lease and never stops a server other sessions use.
- [ ] Turn ids `Strong` from the `turn/start` `seq`.
- [ ] Pass Harness approval outcomes through as option ids.
- [ ] Rollback and fork through `session.fork { atSeq }`, returning `replaced: true`.
- [ ] Map `session/jobs` to subagents and background work.
- [ ] Agent preset changes → `CreateWithHandoff`; presets stay a provider-specific option.
- [ ] Fixtures: all R; S for approval, plan_questions, steering, rollback, fork, background_work, subagent.

## Resolved questions

1. **Async runtime.** Drivers move onto tokio together with the daemon socket rewrite (#18). The trait is async (D-1, D-2). The session manager hosts the runtime until #18 lands, so ports are not blocked on it. Rationale: see "Threading and async model".
2. **Claude: stream-json or the Agent SDK protocol.** Stay on stream-json and implement the needed control-protocol messages and flags in Rust (D-3). The SDK protocol is this protocol; adopting the SDK itself would only add a Node sidecar. Rationale and the feature list: see "Claude transport".

## Open questions

None block the Phase 2 ports. Two facts are left to the port PRs to establish with fixtures: whether Claude's `--resume-session-at` and `set_permission_mode` behave as the SDK uses them (#47), and whether Pi's tool-hook approvals land within #50.

## Amendments made during implementation

The sketches above are kept as written; where the code differs, this list is the contract.

### Trait, types and placement (#44)

- **Where things live.** The trait, session types, transports, `IdAllocator`, coalescers, `V1Bridge`, `ProviderSessionManager` and replay are in `waku-core::driver::v2` as above. The types `decide_ingest` reads are in `waku-orchestrator` (no async runtime, R9) and re-exported from `driver::v2`: `AdapterEvent`, `TurnTerminal`, `TurnOutcome`, `ThreadDisposition`, `RunRef`, `NativeRef`, `ProviderTurnRef`, `BackgroundWorkItem`, `ADAPTER_EVENT_CAPACITY` (`waku_orchestrator::provider`) and `TransitionPlan` (`waku_orchestrator::decide::policy`). `Capabilities` stay in `waku-protocol::orchestration::capabilities` (#42).
- **`DriverV2::kind()` returns `DriverFamily`** (`Codex`, `Claude`, `Acp`, `OpenCode`, `Pi`, `Amp`, `DeepSeek`, plus `Test` for the in-repo fake). `DriverKind` already names the per-flavor enum of the orchestration types, which `InstanceConfig::provider` uses.
- **`AdapterEvent` variants have named fields** and serialize internally tagged (`{"type": "turn_terminal", "terminal": {…}}`), so events can be logged and compared. `TurnOutcome::Failed { failure }` likewise. `CommandOutputDelta.chunk` is `Vec<u8>`: the runtime-free crate takes no `bytes` dependency.
- **Failures use the orchestration `ProviderFailure`** (`code` for `native_code`, `reset_at` in epoch milliseconds, `retryable: Option<bool>`); `FailureClass::Unknown` is that open enum's fallback. `DriverError::to_failure()` maps every error to one; `Unsupported` is a `ValidationError`, transport loss a `TransportError`, everything unclassified `Unknown`.
- **`Clock` is the orchestrator's** (`now() -> Timestamp`, epoch ms), so one `ManualClock` drives the orchestrator, the drivers and the coalescers in tests.
- `InstanceConfig`: `provider: orchestration::DriverKind`, `env: Vec<(String, String)>`, `cli_version: Option<String>` (no `semver` dependency yet). `SessionContext.shutdown` is a small `Shutdown` signal (`tokio-util` is not in the lockfile); `output: Arc<dyn OutputSink>`, `raw_log: Arc<dyn RawFrameLog>`; `mcp: Option<McpInjection>` is a placeholder until #84. `Attachment` is `{ name, mime, path, is_image }` until blobs reach drivers (#78).
- **Transports.** `ProcessSpec` args and env are strings; `StdioTransport.exit` resolves to `ProcessExit { success, code, signal }` (replay cannot build a `std::process::ExitStatus` portably); `HttpMethod` is an enum. `ProcessTransportFactory` spawns through `command_env` (the v1 drivers' login-shell environment and search path) and never kills on drop; its `http` returns `Unsupported` until the first HTTP port (#49, #52). Replay implements both.
- **Synthetic context** uses `WAKU_CONTEXT_V1` through `driver::v2::wrap_synthetic_context`. Context block keys name the purpose (`fork`, `rollback`) and never carry app ids: provider frames must not contain app ids, which the recorder's UUID renumbering would rewrite.
- **`DeltaCoalescer`** is `driver::v2::coalescer::DeltaCoalescer` (16 ms per item, flushed before any other event and before the terminal), built on `SnapshotCoalescer`, which the session manager also uses for R12.

### `ProviderSessionManager` (#44)

- It lives in `waku-core::driver::v2::manager` (RFC 0001's layout put it in `waku-orchestrator::provider`): it owns the drivers and the runtime. `ManagerExecutor(Arc<ProviderSessionManager>)` is the `provider` half of `DefaultExecutor` (RFC 0001 amendment); `DriverRegistry` is also the orchestrator's `CapabilitySource`.
- Effects: `provider_turn.start` (open or reuse the thread's session, apply the selection transition, bind the provider thread with `ensure_thread`/`resume_thread`, `start_turn`; a refusal or error reports `ProviderTurnFailed`), `provider_turn.steer`, `provider_turn.interrupt` (or `stop_subagent`), `provider_turn.restart` (interrupt with `restart_follows`, wait for that attempt's terminal, start the next attempt with the run's newest user message), `runtime_request.respond`, `provider_thread.rollback`, `provider_session.detach`. `provider_runtime.continue` fails permanently until #59; `delegated_tasks.stop` is a no-op until Phase 3. `compact`, `fork_thread`, `unload_thread`, `release_idle` and `shutdown` are manager methods (there is no compaction effect yet, #76).
- **Events.** One pump task per session: `CommandOutputDelta` → `OutputSink`; every event → `subscribe()` (a lossy broadcast the stream work, #54, turns into live `delta` items); everything else → the R12 persist throttle (250 ms per item, only at a finished line, at once on a status change or any other event) → `Orchestrator::ingest` on the blocking pool, in order. Events are scoped to the session's current run when they name it, so a late frame of a finished attempt ingests with no run and is dropped. A terminal naming a subagent's turn (a `ProviderTurnUpdated` without a run attempt) does not end the session's turn.
- **Lifecycle.** One live session per app thread. Release after 30 minutes without a turn, or up to 4 hours while `has_pending_background_work()`; `unload_thread` closes it on demand. A stream that ends without a close dispatches `SessionLost` (command id `session-lost:<session>:<generation>`); the next turn reopens and resumes the native thread.
- **Policies** (pure, `waku_orchestrator::decide::policy`): `steering_policy`, `fork_strategy`, `rollback_strategy`, `session_transition`, `effective_runtime_mode`, `handoff_budget`. `ApplyInSession` without `sessions.model_switch_in_session` restarts instead; `CreateWithHandoff` restarts and resumes until provider switching lands (Phase 3); `Reject` fails the run with a `ValidationError`.
- **Degradation as implemented.** A fork without `threads.fork` (or without `fork_from_turn` for a mid-thread fork) queues a portable transcript for the target thread's first turn (`ContextKind::SyntheticUser`, newest text kept within `handoff_budget`). A rollback without `threads.rollback` closes the session, marks the provider thread `Closed` (so the next run opens a fresh one) and queues a checkpoint-summary handoff of the runs up to the target. Both handoffs are held in memory by the manager until the next turn; Phase 3 persists them as `ContextHandoff` rows.
- A steer the provider rejects (the turn settled first) is logged; promoting the message to a queued run is Phase 3 queue editing.

### `V1Bridge` (#44)

- `V1Bridge::start(driver, instance, DriverStartOptions, DriverEventSender, transports, runtime)` opens the session (resuming `provider_cursor` when given) and serves `DriverControl`. Commands go to one actor task in order; `apply_options`, `rollback` and `fork` wait for its reply. It inverts the table above; subagent and background-work events are not mapped back yet (no port needs them on the bridge before #71).

### Replay (#45)

- **Placement.** Harness, recorder and replay tests are in `waku-core` (`driver::v2::replay::{harness, record}`, `crates/waku-core/tests/replay.rs`), not `waku-orchestrator::testkit`: the orchestrator cannot depend on `waku-core`. There are no `replay` / `replay-recorder` features; the module is small and always built, so `cargo test --workspace` runs every fixture. `ReplayProfile` is in `replay/mod.rs`.
- **Channels.** The n-th process spawned under one channel name in a transcript is `<name>#<n>` (a reopened session, another thread's session); recorder and replay apply the same rule.
- **`runtime_exit`.** `error` ends the channel at once (a crash). `success` and `cancelled` wait until the driver closes the channel's stdin (a graceful shutdown), then end it.
- **Determinism without a paused clock.** The harness uses a multi-thread runtime with real time: effect workers are OS threads, and tokio's paused clock auto-advances whenever the runtime idles while they work. What could vary is pinned instead: the `ManualClock` (2026-01-01T00:00:00Z) moves only with steps and `afterMs`, app ids come from counters (`SequentialIds`, `SeqIdAllocator`), native ids bind through UUIDv5, and every step waits until nothing moves. The coalescers read that clock, so their intervals never fire on their own during replay. `replay_is_deterministic` replays every fixture twice and compares the event logs.
- **Steps.** Added `release_idle`, `unload`, `compact`; `select` takes `{model, reasoningEffort}`; `rollback` takes `restoreFiles`. `restart_daemon` waits for #59. `ScenarioResult` also carries every thread by name, fork results, the checkpoint port's calls and released threads; `checkpoints` are the entities.
- **Fixtures in CI.** The fake provider records 19 scenarios (the orchestrator suite through the trait). "At least one fixture per driver" is met per port: each of #46–#52 records its R and S rows with `waku-replay-record --provider <id>`, which refuses providers without a `DriverV2` port.

### ACP family (#48)

- **JSON-RPC without the SDK connection.** The `agent-client-protocol` SDK allocates UUIDv4 request ids, which replay cannot match literally ("Matching", rule 5), and its connection is `!Send`. The ACP driver (`driver/acp/`) therefore speaks ACP's JSON-RPC itself. Requests take ids from 1 per process, and the actor is an ordinary tokio task, not a dedicated thread with a `LocalSet` (rule 5 of "Threading" no longer applies). The SDK's schema types still decode typed payloads (session config options, modes), and its connection still serves the session catalog (`acp_session`).
- **Flavors.** `InstanceConfig::provider` selects Cursor, Grok, Kimi or Fx. Any other provider is the generic flavor (replay id `acp`): it has no extension methods, and its launch arguments come from `extra.args`.
- **Capabilities.** The matrix column as written. `read_snapshot` (and `provider_can_read_conversation_snapshot`) comes from `initialize`'s `loadSession`. `model_switch_in_session` (note 1) is decided per flavor: the option set arrives only with `session/new`, after the capabilities are fixed, so Cursor, Grok, Kimi and Fx declare Y and the generic agent declares N (it restarts on a model change). `computer_use` is Grok's only with Computer Use enabled. The snapshots are `driver/acp/snapshots/capabilities_<flavor>.json`.
- **Turns.** The driver reports the running provider turn itself, before it writes `session/prompt`. Grok's `_x.ai/session/prompt_complete` settles a turn before the RPC response does. Its `rate_limit` stop reason is `UsageLimit` and its `error` stop reason is `ProviderError`. `refusal`, `max_tokens` and `max_turn_requests` are `Failed { ProviderError }`. Unknown stop reasons complete. `UsageLimit` is also classified from a JSON-RPC error: code `-32003` for Grok, or `http_status: 429` or a limit code in `error.data` for any flavor. `reset_at` comes only from `resetAt`/`resetsAt`/`reset_at` or `retryAfterSeconds`/`retry_after` in `error.data`. Kimi's wire-log failure keeps its code and is `UsageLimit` for a limit code.
- **Selection.** `ApplyOnNextTurn` changes (service tier) reach the agent at the next `start_turn`, which re-applies the model choice when it differs from the one last applied.
- **Approvals.** Outside Supervised mode the driver answers `session/request_permission` itself with the agent's `allow_always`, else its `allow_once`, as the v1 driver did. Request ids bind as `<provider session>:<agent id>`, because agents restart their ids with every process.
- **Grok fork and rollback.** Rollback to the thread start opens a fresh native session on the live connection; anything else forks on channel `fork-1`. The live connection switches to the replacement lazily: the next turn's `session/resume` (or `session/load`) activates it.
- **V1Bridge.** `driver::acp::start` runs the driver behind `V1Bridge` on an ACP-owned runtime. Three things have no `AdapterEvent` yet and reach the desktop through a legacy-only sink: `AvailableCommands`, the native title (`session_info_update`) and `Connected` with the resume cursor. Grok's polling of `summary.json` for its title is gone, because `session_info_update` carries it. Behind the bridge the session is created at startup, as in v1. A Cursor branch cursor's retained history is sent with the first prompt in the `WAKU_CONTEXT_V1` envelope instead of Cursor's own envelope. `daemon.rs` keeps its Grok fork and rewind arms until the cutover; they call `driver::grok_fork_session_at_turn`.
- **Replay.** The harness gains a `runtime_mode` step (Supervised approvals). Per-provider `input.<provider>.json` overrides the shared steps where an agent's own option or question ids are needed (`tests/replay_acp.rs`). The ACP fixtures are adapted from T3's recorded Grok transcripts or written from the spec. None is a live recording; see `fixtures/replay/ACP_FIXTURES.md`.

- **Per-provider inputs and runtime mode.** A scenario directory may hold `input.<provider>.json`, which replaces the shared `input.json` for that provider (its recording's prompts, native option ids); `ScenarioInput::load` picks it. `ScenarioInput.runtimeMode` sets the main thread's runtime mode before the steps (default full access). The sanitizer redacts only *string* values under secret-looking keys: `totalTokens` and the other token counts are numbers and survive.

### Codex (#46)

- **Layout.** `driver/codex/{mod.rs, protocol.rs, normalize.rs}` as step 1 says, plus `legacy.rs` (the `V1Bridge` wrapper), `title.rs` (the `TitlePort`'s one-shot title, unchanged) and `script.rs` (below). `codex_session::fork_session_at_turn` is gone; `list_provider_sessions` and `provider_session_history` stay for cold imports.
- **Rollback is `thread/revert`, not `thread/rollback`.** Codex 0.156 (T3's recordings) and 0.159.2 (`codex app-server generate-json-schema --experimental`) have no `thread/rollback`; `thread/revert {threadId, beforeTurnId}` replaces a paginated thread's history with the prefix before a turn. The driver pages `thread/turns/list` newest first (as T3 does) to find that turn. The count is the native turns this session saw after the target, so `/goal` continuations Codex started itself are dropped with their neighbours; for a thread resumed in this session it falls back to the orchestrator's `turns_to_remove`. `retained_native_turn_ids` lists the turns left when the session knows them.
- **`provider_rollback_returns_snapshot = N`** (the matrix said Y). The revert response carries the thread without turns, and `CheckpointRollbackApplied` replaces the provider-thread row wholesale with a snapshot's row, which the driver cannot build without the app-side fields (`forked_from`, run ordinals, handoffs). `read_snapshot` (`thread/read`, `includeTurns`) still returns one.
- **Fork** is `thread/fork {threadId, lastTurnId?, cwd, excludeTurns, deferGoalContinuation}` on the live connection, followed by `thread/unsubscribe` of the new thread: Waku runs the fork's turns in a session of its own, and a thread stays loaded (with its writer) in the app-server that created it. `thread/resume` retries for about two seconds when Codex reports the thread still loaded elsewhere. That release has not been checked against a live CLI (see the PR). `ForkPoint::SubagentThread` forks the subagent's native thread; `fork_from_subagent_thread` stays N until a harness step can prove it.
- **Root provider-thread rows.** The driver emits `ProviderThreadUpdated` only for subagent threads (which it creates whole) and, behind `V1Bridge`, for the root thread (the bridge turns its cursor into `Connected` and its name into the auto title). In orchestrator mode the manager writes root rows, and titles come from the `TitlePort`, so `thread/name/updated` is not mapped there. `mcpServer/startupStatus/updated` (failed) is `SessionUpdated { status: Ready, last_error }`.
- **Turns Codex starts itself** (`/goal` continuations) are `ProviderTurnUpdated { run_attempt_id: None }` with no `TurnTerminal`, so they never count towards `provider_turn_ordinal`; their items wait for #59. Behind `V1Bridge` each gets a run of its own, so the desktop sees it start and finish as before, and Stop reaches it.
- **Approvals.** The option ids are the request's own `availableDecisions` (`accept`, `acceptWithExecpolicyAmendment`, `cancel`, …), passed through, the reply carrying the decision value verbatim; requests without the list offer `accept`, `acceptForSession`, `decline`. `item/permissions/requestApproval` answers with a grant (`{permissions, scope}`). Request app ids bind on the item id (and approval id), not the per-connection JSON-RPC id, which repeats across sessions. `mcpServer/elicitation/request` is declined automatically (a declared gap) and any other server request gets a JSON-RPC error, so a turn never hangs on a request Waku cannot show. `serverRequest/resolved` resolves a request answered elsewhere.
- **Async questions** (an `agentMessage` with `delivery: "async"` and `questions`) become `RuntimeRequest { kind: UserInput, response_capability: Message }` with a `response_mode: message` item; `respond` refuses them (`Unsupported`) until answering by message lands (Phase 3).
- **Usage.** `thread/tokenUsage/updated` sets the turn's `token_usage.used` to `last.totalTokens` (what fills the window now), its counters from `total`; `turn_token_usage` is `last`. `account/rateLimits/updated` snapshots merge (an omitted field keeps its value; another `limitId` is ignored, as in T3) and `reset_at` of a `UsageLimit` is the latest `resetsAt` of the exhausted windows. Failures classify from `codexErrorInfo` only, merged from the `error` notification when `turn/completed` lacks it.
- **Background work.** A command still running when its turn *completes* is `BackgroundWorkUpdated { live: true }` (task id = `processId`) until its `item/completed` (`live: false`); `stop_background_work` is `thread/backgroundTerminals/terminate`. Interrupted or failed turns close their open tools as interrupted/failed. `background_pending` stays false: Codex never wakes the session on its own for a terminal.
- **`extras.computer_use`** follows `InstanceConfig.computer_use_enabled` (debug builds) instead of a constant Y.
- **The bridge for Codex.** `start_local` opens the session through `V1Bridge::start` on a small runtime of its own (`waku-codex`, 2 workers) until the daemon's runtime is shared with drivers. `legacy.rs` adds what the bridge does not translate yet, ordered with the bridge's events: the old `RichActivity` presentation replaces the bridge's plain `Activity` of the same native item, `BackgroundWork` events for commands and subagents (and the refresh/stop commands), the first prompt's generated title (`thread/name/set`), visible stderr notices, the Computer Use preview monitor, a quiet `thread/goal/get` after a resume, and Stop for a turn Codex started itself.
- **Fixtures.** No live recording was made (it would spend the owner's quota). `waku-replay-record --provider codex --script T` plays a transcript's inbound frames through `ScriptedAppServer` (outbound frames matched by method, request ids mapped) and records the driver's real outbound frames; T3's recorded Codex transcripts are the scripts. `fixtures/replay/CODEX.md` lists each scenario's source and every adaptation; the headers say `synthetic: true`. Codex records 15 RFC scenarios plus `todo_list`.

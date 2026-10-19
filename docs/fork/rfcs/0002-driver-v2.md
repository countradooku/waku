# RFC 0002: `DriverV2`, normalized adapter events and capability groups

- **Status:** Draft. Tracked in [#12](https://github.com/countradooku/waku/issues/12).
- **Implements:** [#44](https://github.com/countradooku/waku/issues/44) and [#45](https://github.com/countradooku/waku/issues/45), plus the per-driver port issues in Phase 2.
- **Reference:** T3 `packages/provider-core/src/server/ProviderAdapter.ts` ([audit](../audit/t3-providers.md) §2, [details](../audit/t3-provider-adapters-detail.md)).

## Problem

Waku drivers implement `DriverControl` (prompt, steer, cancel, respond, respond_user_input, goal, apply_options, rollback, fork) and emit `DriverEvent`s.

- The desktop interprets those events itself.
- Capability flags such as "can rewind" are static on `ProviderKind`.
- The orchestrator (RFC 0001) needs drivers that emit app-level entity updates and declare per-session capabilities, so policy, not provider names, decides behaviour.

## Trait

```rust
pub trait DriverV2: Send + Sync {
    fn kind(&self) -> DriverKind;
    fn capabilities(&self, cfg: &InstanceConfig) -> Capabilities;
    fn plan_selection_transition(&self, from: &Selection, to: &Selection) -> SessionTransition;
    fn open_session(&self, ctx: SessionContext) -> Result<Box<dyn DriverSession>>;
}

pub trait DriverSession: Send {
    fn events(&mut self) -> EventStream<AdapterEvent>;          // normalized, see below
    fn ensure_thread(&mut self, t: ThreadOpen) -> Result<ProviderThreadRef>;
    fn resume_thread(&mut self, r: &ProviderThreadRef) -> Result<()>;
    fn start_turn(&mut self, t: TurnStart) -> Result<()>;        // message, attachments (native images), context envelope
    fn steer_turn(&mut self, s: Steer) -> Result<()>;            // only if caps.turns.active_steering
    fn interrupt_turn(&mut self, run: RunRef) -> Result<()>;     // terminal arrives later via TurnTerminal
    fn respond(&mut self, r: RuntimeResponse) -> Result<()>;     // approvals, questions (+attachments)
    fn read_snapshot(&mut self, t: &ProviderThreadRef) -> Result<ThreadSnapshot>;
    fn rollback(&mut self, t: &ProviderThreadRef, to_turn: u32) -> Result<ThreadSnapshot>;
    fn fork(&mut self, f: ForkRequest) -> Result<ProviderThreadRef>;
    // optional, gated by capabilities
    fn compact(&mut self, _t: &ProviderThreadRef) -> Result<()> { Err(Unsupported) }
    fn inject_history(&mut self, _h: Handoff) -> Result<()> { Err(Unsupported) }
    fn stop_subagent(&mut self, _s: SubagentRef) -> Result<()> { Err(Unsupported) }
    fn unload_thread(&mut self, _t: &ProviderThreadRef) -> Result<()> { Ok(()) }
    fn has_pending_background_work(&self) -> bool { false }
}
```

Errors are typed: `Unsupported`, `NotReady`, `SessionLost`, `ProviderError{class, retryable, reset_at}`. The `class` is one of usage_limit, provider_error, transport_error, permission_error, validation_error or unknown.

## Normalized events

```rust
pub enum AdapterEvent {
    SessionUpdated(ProviderSession),
    ProviderThreadUpdated { thread: ProviderThread, retained_native_turn_ids: Vec<String> },
    ProviderTurnUpdated(ProviderTurn),               // token usage, context usage
    NodeUpdated(ExecutionNode),
    SubagentUpdated(Subagent),
    MessageUpdated(Message),
    TurnItemUpdated(TurnItem),
    CommandOutputDelta { item: TurnItemId, bytes: Bytes },   // never persisted; goes to the output hub
    RuntimeRequestUpdated(RuntimeRequest),
    PlanUpdated(Plan),
    TurnTerminal { run: RunRef, outcome: Completed | Interrupted | Cancelled | Failed(ProviderFailure),
                   thread_disposition: Reusable | Broken, child_turns_continue: bool },
}
```

Rules:
- **Entity snapshots.** Events are entity snapshots keyed by app ids. The driver resolves native ids to app ids through the scoped `IdAllocator`, and replaying the same frame finds the same binding.
- **Ordering.** `TurnTerminal` is authoritative. Status-idle notifications never complete a run.
- **Child turns.** A child (subagent) turn's terminal event never completes the parent.
- **Option ids.** Native option ids for permissions and questions survive normalization; labels are not valid replies.

## Capabilities

There are 12 groups, each with explicit booleans or enums:

- **sessions:** multiple threads per session, model switch in session, runtime-mode switch, pending requests survive restart.
- **threads:** create empty, read snapshot, rollback, fork, fork from turn, fork from subagent, native thread id.
- **turns:**
  - native turn id;
  - started/completed events;
  - interrupt;
  - active steering, and whether steering interrupts tools;
  - steer via interrupt-restart;
  - queued messages;
  - terminal status quality.
- **streaming:** text deltas, reasoning deltas.
- **tools:** item ids, started/completed/output, MCP tools, dynamic tool callbacks.
- **approvals:** command, file read, file change and apply_patch approvals; native request ids; live-only callbacks; approvals from subagents.
- **planning:** plan updates, todo lists, proposed plans, structured questions.
- **subagents:** native subagents, child transcripts, stop.
- **context:** system/developer context, summaries, delta or full handoff, `max_recommended_handoff_chars`, native history injection.
- **checkpointing:** app filesystem checkpoint, nested scopes, provider rollback, rollback snapshot.
- **identity:** strength of native thread, turn, item and request ids.
- **runtime_policy:** enforcement is `Native` or `ClientBoundary`. A decoder default of `ClientBoundary` keeps replay from over-claiming.

## Degradation policies (orchestrator side)

| Missing capability | Policy |
|---|---|
| Active steering | Interrupt and restart within the same run (new `RunAttempt{steering_restart}`) |
| Native fork | Portable context handoff on the first run of the fork |
| Native rollback | Fresh provider thread plus checkpoint-summary handoff |
| Native approvals | Client-boundary enforcement, or a full-access-only warning |
| Images | Path mention fallback |

## Mapping from current Waku drivers

| Waku today | DriverV2 |
|---|---|
| `prompt` | `start_turn` |
| `steer` | `steer_turn` |
| `cancel` | `interrupt_turn` |
| `respond`, `respond_user_input` | `respond` |
| `rollback(n)` | `rollback` |
| `fork` | `fork` |
| `goal` | `start_turn` with a goal intent (Codex/Claude) |
| `apply_options` | `plan_selection_transition` plus in-session switch |
| `ProviderKind::supports_*` | `Capabilities` |
| `DriverEvent::*` | `AdapterEvent::*`, normalized |
| Computer-use hooks | Stays a driver extension, gated by settings |

Per-driver notes:

| Driver | Notes |
|---|---|
| Codex | Richest protocol; all flags available. |
| Claude | Steering via stream input; `resumeSessionAt` rollback; `forkSession`. |
| ACP family | Steer by cancel and restart; rollback and fork only where advertised. |
| OpenCode | Native rollback and fork. |
| Pi / Oh My Pi | Session files back rollback and fork; approvals through a tool hook. |
| Amp, DeepSeek, Kimi, Fx | Declare gaps honestly. |

## Replay fixtures

The transcript is JSONL. Each entry is one of:
- `{"t":"expect_outbound","frame":…}`
- `{"t":"emit_inbound","frame":…,"delay_ms":0}`
- `{"t":"runtime_exit","code":…}`

How fixtures are used:
- **Recording:** a recorder wraps the real transport once; fixtures are sanitized (paths, tokens) and checked in under `crates/<driver>/fixtures/`.
- **Running:** the harness swaps only the transport and runs the real orchestrator, sink, projections and checkpoints with a deterministic clock and ids.
- **Mismatches:** an outbound mismatch fails with a frame diff.
- **Coverage:** each driver records at least a plain turn, tool use, an approval, an interrupt, a steer or restart, rollback and fork where supported, and a usage-limit failure.

## Open questions

1. Async runtime: keep blocking threads per session, or move drivers onto tokio together with the daemon socket rewrite ([#18](https://github.com/countradooku/waku/issues/18))?
2. Claude: stay on stream-json, or adopt the Agent SDK protocol for parity with T3 features such as workflows and background tasks?

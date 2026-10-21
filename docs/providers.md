# Provider integrations

How Waku talks to each coding agent: the process it launches, the wire protocol
it speaks, how long that process lives, and what has to be emulated because the
CLI does not offer it.

How each of them names a session — which are read from the provider, which are
polled off disk, and the one Waku generates itself — is in
[titles.md](titles.md).

Every provider is reached through the same driver abstraction, `DriverV2`
(RFC 0002), in [driver/v2/mod.rs](../crates/waku-core/src/driver/v2/mod.rs).
There are seven transport implementations behind eleven providers, and **every
one of them holds a session that spans the whole conversation**:

| Transport | File | Providers |
| --- | --- | --- |
| Codex app-server (JSON-RPC over stdio) | [driver/codex/](../crates/waku-core/src/driver/codex/mod.rs) | Codex CLI |
| Agent Client Protocol (JSON-RPC over stdio) | [driver/acp/](../crates/waku-core/src/driver/acp/mod.rs) | Cursor CLI, Fx, Grok Build, Kimi Code |
| OpenCode background service (HTTP + server-sent events) | [driver/opencode/](../crates/waku-core/src/driver/opencode/mod.rs) | OpenCode |
| Pi RPC mode (NDJSON request/response over stdio) | [driver/pi/](../crates/waku-core/src/driver/pi/mod.rs) | Pi, Oh My Pi |
| Claude streaming-input session (NDJSON over stdio) | [driver/claude/](../crates/waku-core/src/driver/claude/mod.rs) | Claude Code |
| Amp streaming-JSON session (NDJSON over stdio) | [driver/amp/](../crates/waku-core/src/driver/amp/mod.rs) | Amp |
| Harness client API (typed HTTP + WebSocket downlinks) | [driver/deepseek/](../crates/waku-core/src/driver/deepseek/mod.rs) | DeepSeek Harness |

Every driver implements only `DriverV2` and is driven by the orchestrator's
`ProviderSessionManager`
([driver/v2/manager.rs](../crates/waku-core/src/driver/v2/manager.rs)).
Clients (desktop, web) speak protocol v8 (RFC 0003) to the daemon and never
reach a driver directly.

DeepSeek Harness has no dedicated section below yet; its driver's module
comment and RFC 0002's amendments (#52) are the current reference. Since
the `DriverV2` port each session holds a lease on the pooled `dsh web` and
opens its own `/api/events.mux` and `/api/events.host` WebSockets through
`driver::v2::http`; the Host process no longer runs reader threads. Approval
outcomes are passed through as option ids (`allowed-once`, `rejected`, or
whatever the request lists), agent presets are fixed at `session.create`
(a change starts a new provider thread with a handoff), and capabilities are
in [capabilities_deepseek.json](../crates/waku-core/src/driver/deepseek/capabilities_deepseek.json).

## The driver contract

`DriverV2` is one implementation per transport family and holds no session
state. `open_session(SessionContext)` starts or attaches the native runtime,
completes the handshake and returns an `OpenedSession`: a `DriverSession`
actor, the bounded channel of `AdapterEvent`s it emits, and the capabilities
negotiated in the handshake. Drivers never spawn a process or open a socket
themselves; they go through `ctx.transports`, which replay replaces.

The session's methods are the provider-control surface: `ensure_thread`,
`resume_thread`, `start_turn`, `steer_turn`, `interrupt_turn`, `respond`
(approvals and questions), `read_snapshot`, `rollback`, `fork`,
`apply_selection`, and the optional `compact`, `inject_history`, `goal`,
`stop_background_work` and `stop_subagent`, each gated by a capability.
`start_turn` resolves when the provider accepts the input; the turn ends with
exactly one `AdapterEvent::TurnTerminal`.

Outputs are `AdapterEvent`s
([waku-orchestrator/src/provider.rs](../crates/waku-orchestrator/src/provider.rs)):
entity snapshots keyed by app ids — `SessionUpdated`, `ProviderThreadUpdated`,
`ProviderTurnUpdated`, `NodeUpdated`, `SubagentUpdated`, `MessageUpdated`,
`TurnItemUpdated`, `RuntimeRequestUpdated`, `PlanUpdated`,
`BackgroundWorkUpdated`, `AvailableCommandsUpdated`, `GoalUpdated` — plus
`CommandOutputDelta` and `AccountUsageUpdated`, which the manager fans out and
never ingests, and the turn's `TurnTerminal`.

How a message reaches a running turn is the orchestrator's
`steering_policy`, read off the session's capabilities: a session that
declares `turns.active_steering` gets `steer_turn` into the live turn (which
fails with `SteerRejected` once the turn has settled); one that declares
`turns.steering_by_interrupt_restart` (or only `turns.interrupt`) is
interrupted and restarted within the same run as a new
`RunAttempt { steering_restart }`; anything else queues the message until the
turn ends.

Every driver normalizes its tool events through
[driver/activity.rs](../crates/waku-core/src/driver/activity.rs) and
[driver/tool_items.rs](../crates/waku-core/src/driver/tool_items.rs) into
`TurnItemUpdated` items, so the transcript renders provider-agnostic rows.
Tool titles prefer a `title` argument when the tool supplies one, then fall
back to the command, the query, or a de-camel-cased tool name.

### Session lifetime

The `ProviderSessionManager` opens a session lazily per app thread, the first
time an effect needs one, and keeps one live session per thread. Sessions run
on the shared driver runtime (`driver::v2::runtime()`), not on any client:
switching threads in a client does not touch them, so a background thread keeps
streaming.

A live session — and with it the provider process — is closed (`close_live`)
when:

| Trigger | Where |
| --- | --- |
| The thread's provider instance changes | `ensure_live` |
| A selection change the session cannot absorb (see below) | `ensure_live`, `restart` |
| An interrupt cannot be delivered (the manager then reports the turn's terminal itself) | `interrupt` |
| A rollback the provider cannot perform in place | `rollback` |
| The thread is unloaded | `unload_thread` |
| No turn for 30 minutes, or up to 4 hours while the driver reports background work | `release_idle`, swept every minute by the daemon |
| The daemon shuts down | `shutdown` |

Stop is `interrupt_turn` on every provider, and the session stays open. Codex
keeps its app-server and the loaded thread after `turn/interrupt`; Amp, which
offers no interrupt on its stream, retires its own process and the next turn
runs `threads continue` (see its section).

Option changes are planned per driver. `DriverV2::plan_selection_transition`
is a pure function of the session's capabilities and the old and new
`Selection`; the manager's `session_transition` turns the plan into reuse (the
change rides on the next turn), an in-session switch through
`apply_selection`, a restart that resumes the same native thread, or a
rejection:

| Change | Codex | Pi | ACP | OpenCode | Claude | Amp |
| --- | --- | --- | --- | --- | --- | --- |
| Model | next turn — it rides on every `turn/start` | in session — `set_model` | in session when the agent can switch models (`session/set_model`, Cursor's parameterized `configOptions`, or Fx's advertised `model` option), else restart | in session — `POST /api/session/{id}/model` | in session — a `set_model` control request | restart — a launch argument |
| Reasoning effort | next turn | in session — `set_thinking_level` | in session as a config option; restart for Grok, which takes it at launch | next turn — the model's variant | restart | restart |
| Service tier | next turn | next turn | next turn | next turn | next turn | restart |
| Access mode | restart | restart | restart | in session — the mode only decides who answers a permission request, which is the driver's own state | restart | restart, and only Full access is accepted |
| Provider | new session | new session | new session | new session | new session | new session |

The permission policy is deliberately excluded even for Codex, which does carry
`approvalPolicy` and `sandboxPolicy` on every `turn/start`: loosening or
tightening what an already-running agent may touch deserves a fresh session,
which resumes the same native thread.
OpenCode is the exception because nothing about the mode reaches the provider:
its agent's own rules still decide what to ask, and Waku only decides whether
to ask the user or answer `once` itself. T3
Code draws the line in the same place — it restarts on `runtimeModeChanged` and
keeps the session only for a model change the adapter declares it can switch.

The idle sweep skips any session with a running turn, so a slow tool call or an
unanswered approval is never reaped out from under the user.

Note what is *not* on the teardown list: finishing a turn. `TurnTerminal` leaves
the long-lived processes resident and idle, which is the point of them — until
the idle sweep decides otherwise.

### How the long-lived processes actually die

Two shapes, depending on the transport.

**The stdio drivers — Codex, Pi, Claude, Amp, and the ACP agents — are never
signalled** (except Amp's interrupt, below).
Termination is by **closing stdin** (RFC 0002 "Threading" rule 2):

1. The manager's `close_live` calls `DriverSession::close()`.
2. The session closes the `stdin` of its `StdioTransport`.
3. The provider sees EOF on stdin and exits.
4. Its stdout closes, ending the session's read loop.

So the process is asked to leave by having its input closed, and a provider that
ignored stdin EOF would linger. When the daemon exits without running
`shutdown`, the OS closes the descriptors, which is the identical signal.

Every `StdioTransport` carries an `exit` future that awaits the child
(`tokio::process::Child::wait`), so the process is reaped and its exit status
is available to the driver. The transport never kills a child on drop:
`close()` owns shutdown.

**OpenCode is different again**: Waku does not own the process at all. Its
background service belongs to the user and outlives every Waku task, so
closing a session only drops that session's own `GET /api/event` downlink.
Waku never signals the service, never calls its stop route, and never writes
its registration file
([opencode_service.rs](../crates/waku-core/src/opencode_service.rs)).

The one explicit signal is narrow and deliberate: Amp's interrupt sends SIGINT
through the transports and retires the process. The short-lived Grok process
that backs a fork is closed by its stdin like every other.

## At a glance

| | Codex CLI | Pi | Oh My Pi | Claude Code | Amp | Cursor CLI | Fx | OpenCode | Grok Build | Kimi Code |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Binary | `codex` | `pi` | `omp` | `claude` | `amp` | `cursor-agent` | `fx` | `opencode` | `grok` | `kimi` |
| Wire protocol | JSON-RPC over stdio | NDJSON RPC over stdio | NDJSON RPC over stdio | stream-json over stdio | stream-json over stdio | ACP over stdio | ACP over stdio | HTTP + SSE | ACP over stdio | ACP over stdio |
| Process spans the whole session | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes |
| Process spawned per turn | no | no | no | no | no | no | no | no | no | no |
| Bidirectional | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes |
| Reasoning stream | yes | yes | yes | yes | yes | yes | yes | yes | yes | yes |
| Interactive approvals | yes | yes (Waku's extension, #50) | yes (`--approval-mode`, #50) | yes | no | yes | yes | yes | yes | yes |
| Mid-turn steering | native | native | native | native | native | interrupt and restart | interrupt and restart | native | interrupt and restart | interrupt and restart |
| Model discovery | yes | yes | yes | no (fixed) | no (modes) | yes | yes | yes | yes | yes |
| Computer Use | yes | no (dropped by the `DriverV2` port, #50) | no (ships its own) | no | no | no | no | no (dropped by the `DriverV2` port, #49) | no (its setup was removed with `V1Bridge`) | no |
| Restricted to Full access | no | no | no | no | yes | no | no | no | no | no |
| Rewind and branch at a turn | yes | yes | yes | yes | handoff (orchestrator, #51) | handoff (orchestrator, #48) | **no** | yes | yes | **no** |

The ACP agents declare `turns.active_steering = false` and
`turns.steering_by_interrupt_restart = true` (RFC 0002 D-7, #48): the
orchestrator cancels the running prompt and restarts the run with the new
message. See [Agent Client Protocol](#agent-client-protocol).

Every provider now holds a session across turns. That was not true when this
document was first written: five of the seven spawned a process per prompt, and
everything stateful — resume, rewind, branch, approvals — had to be reconstructed
from a session id, an on-disk transcript, or a side-channel. In each case the CLI
turned out to already serve a session protocol; nobody had looked.

---

## Codex CLI

The Codex driver was the first `DriverV2` port (RFC 0002, #46):
[driver/codex/](../crates/waku-core/src/driver/codex/mod.rs). Like every
driver, it is opened and driven by the `ProviderSessionManager`.

**Launch** — `codex app-server --stdio` through `ctx.transports`, plus `-c`
config overrides (and two environment variables) when Computer Use is on
([computer_use.rs](../crates/waku-core/src/driver/codex/computer_use.rs)), and
`CODEX_HOME` when the instance names a provider home.

**Protocol** — newline-delimited JSON-RPC over stdio, genuinely bidirectional:
Codex sends Waku requests (approvals, questions) and Waku answers them by id.
One actor per session on the driver runtime: a single stdin writer with a
per-process id counter starting at 1, a read loop that applies every frame in
order ([normalize.rs](../crates/waku-core/src/driver/codex/normalize.rs)), and
a stderr reader that feeds the raw log.

**Lifetime** — long-lived: one app-server serves the session, idle between
turns. It ends when the session closes, by closing stdin, never by a signal.
An interrupt keeps the app-server and the loaded thread.

**Handshake** — `initialize` (id 1) with `clientInfo` and
`capabilities.experimentalApi`, `initialized`, then `skills/extraRoots/set`
when Computer Use is on. The thread opens with `thread/start` (`cwd`,
`approvalPolicy`, `sandbox`, `approvalsReviewer`, `serviceName`, optional
`model` / `serviceTier`) or `thread/resume` (`threadId`, `excludeTurns: true`).

**Per turn** — `turn/start` with `threadId`, `input: [{type: "text"}]`,
`approvalPolicy`, `approvalsReviewer`, `sandboxPolicy`, `summary: "auto"` and
optional `model`, `effort`, `serviceTier`. Developer context goes in
`collaborationMode.settings.developer_instructions` when a model is selected;
synthetic context in the `WAKU_CONTEXT_V1` envelope.

**Inbound stream** — normalized to `AdapterEvent`s:

| Method | Becomes |
| --- | --- |
| `turn/started` / `turn/completed` | `ProviderTurnUpdated`; the completion is the turn's one `TurnTerminal` |
| `item/agentMessage/delta`, `item/reasoning/*Delta` | coalesced `MessageUpdated` / `TurnItemUpdated` snapshots |
| `item/started`, `item/completed` | turn items: command, file change, MCP / dynamic tool, web search, image view |
| `item/commandExecution/outputDelta` | `CommandOutputDelta` |
| `turn/plan/updated` | `PlanUpdated` (todo list) |
| `subAgentActivity`, `collabAgentToolCall` items; child-thread turns | `SubagentUpdated`, the child's provider thread and turns |
| `thread/tokenUsage/updated` | the turn's token usage (context meter) |
| `account/rateLimits/updated` | `AccountUsageUpdated`, and the reset time of a usage limit |
| `thread/goal/updated` / `cleared` | `GoalUpdated` |
| `*/requestApproval`, `item/tool/requestUserInput` | `RuntimeRequestUpdated` |
| `error` (no retry) | the failure class of the turn's terminal |

**Approvals** — the request's own `availableDecisions` are the option ids
(`accept`, `acceptForSession`, `acceptWithExecpolicyAmendment`, `decline`,
`cancel`), passed through unchanged; the answer is a JSON-RPC response
`{"decision": …}` (a permission grant for `item/permissions/requestApproval`).
MCP elicitations are declined automatically (a declared gap).

**Steer** — `turn/steer {threadId, expectedTurnId, input}`; an error response
fails `steer_turn` with `SteerRejected`. **Stop** — `turn/interrupt {threadId,
turnId}`, sent as soon as Codex names the turn when Stop came first.

**Rollback** — `thread/turns/list` (newest first) finds the first turn to
drop, then `thread/revert {threadId, beforeTurnId}`, in place. `/goal`
continuations Codex started itself are dropped with the turns around them.
**Fork** — `thread/fork {threadId, lastTurnId?, cwd, excludeTurns,
deferGoalContinuation}`. Codex declares `threads.fork_in_target_session`, so
the manager makes the fork in the target thread's own session: the new thread
is that session's root. **Snapshot** — `thread/read`.

**Background terminals** — a command still running when its turn completes is
background work until its `item/completed`; `thread/backgroundTerminals/list`
and `/terminate` back the panel's refresh and stop.

**Citations** — web citations marked with private-use characters
(`U+E200`/`U+E201`/`U+E202`) are rewritten into markdown links; unknown markers
are dropped. Private control markers never reach the transcript.

**Titles** — a one-shot ephemeral turn on a short-lived app-server
([title.rs](../crates/waku-core/src/driver/codex/title.rs)), used by the
orchestrator's `TitlePort`.

**Models** — a throwaway app-server, `model/list` paged via `nextCursor`, up to
32 pages ([model_catalog.rs:367](../crates/waku-core/src/model_catalog.rs#L367)).

**Replay** — every RFC 0002 scenario Codex needs is under
`crates/waku-core/fixtures/replay/<scenario>/codex.ndjson`; their sources are in
[fixtures/replay/CODEX.md](../crates/waku-core/fixtures/replay/CODEX.md).

---

## Pi and Oh My Pi

One `DriverV2` driver serves both (RFC 0002, #50):
[driver/pi/](../crates/waku-core/src/driver/pi/mod.rs), opened and driven by
the `ProviderSessionManager`.

Oh My Pi is a fork of Pi that kept the RPC transport and renamed part of its
surface. `PiFlavor`
([driver/pi/protocol.rs](../crates/waku-core/src/driver/pi/protocol.rs))
carries every divergence:

| | Pi | Oh My Pi |
| --- | --- | --- |
| Binary | `pi` | `omp` |
| Launch | `--mode rpc --approve` (`--approve` trusts project files; Pi has no permission system) | `--mode rpc` plus `--yolo` (Full access) or `--approval-mode always-ask` / `write` |
| Approvals | Waku's extension (`--extension`), a `tool_call` hook asking through `confirm` | Oh My Pi's own wrapper, asking through `select` with `Approve` / `Deny` |
| Update check | skipped by `PI_SKIP_VERSION_CHECK=1` | gated by a setting, off the startup path |
| Oversized frames | whole | chunked, once `negotiate_protocol {protocolVersion: 2}` is accepted |
| Run settles on | `agent_settled` | `agent_end` (unless `isTerminal: false`) |
| Title event / field | `session_info_changed` / `name` | `session_info_update` / `title` |
| Branch commands | `get_fork_messages`, `fork` | `get_branch_messages`, `branch` |

Neither `--yolo` nor `--fork` appears in `omp --help`, but both are accepted
(verified against 17.3.8); `--approval-mode` is in Oh My Pi 18.4.9's flag
table (`cli/flag-tables.ts`). omp rejects a genuinely unknown flag outright.

**Protocol** — NDJSON over stdio, request/response rather than JSON-RPC: Waku
stamps each command with `waku-<n>` (per process) and Pi answers with
`{"type": "response", "id", "success", "data"}`. Everything else is an event.
Commands time out after 10 s; the handshake gets 30 s, because the agent does
not read stdin until its extensions, resources and model catalog are loaded.

**Session lifecycle** — `open_session` spawns the agent and reads the session
it created (`get_state`: `sessionId`, `sessionFile`, the model's context
window). `ensure_thread` binds that session, or asks for a `new_session` when
the process already served a thread. `resume_thread` is `switch_session
{sessionPath}`: resume needs the session **file**, which is in the cursor.
The process lives until `close()` closes its stdin.

**Turns** — `{"type": "prompt", "message": …}`; `start_turn` resolves at the
prompt's response or the first agent event, whichever comes first, and the
settle event ends the turn. A built-in or extension command that ran locally
answers `agentInvoked: false` and completes with that response (its
`command_output` is the reply). Steering is `{"type": "steer"}`; interrupting
is `{"type": "abort"}` on the same process, which keeps the session. A turn
the agent accepted but never started ends `Cancelled` once the abort is
answered.

**Inbound stream**
([driver/pi/normalize.rs](../crates/waku-core/src/driver/pi/normalize.rs)):

| Event | Becomes |
| --- | --- |
| `agent_start`, `turn_start` | the turn is running (`ProviderTurnUpdated`) |
| `message_update` → `text_delta` / `thinking_delta` | coalesced assistant message and reasoning items |
| `message_end` (assistant) | completes the message; usage → context tokens; `stopReason: error` → the turn's failure |
| `tool_execution_start` / `_end` | tool items (`bash` → command, `edit`/`write` → file change, `grep`/`find` → search, the rest dynamic) |
| `auto_retry_start` / `_end` | usage-limit reset (`delayMs`), or the failure cleared or confirmed |
| `extension_ui_request` (`confirm`, `select`, `input`, `editor`) | approvals and structured questions; `cancel` withdraws one |
| `agent_settled` / `agent_end` | the turn's one terminal |
| title event | the native title, as a partial provider-thread snapshot |

**Approvals** — Pi leaves permissions to extensions, so in a supervised mode
the session loads Waku's extension (written to `~/.waku-fork/pi/` on first use)
with `WAKU_PI_RUNTIME_MODE`; its `tool_call` hook asks `Allow <tool>?` and
blocks a declined call. Read-only tools never ask; `auto_accept_edits` lets
`edit` and `write` through. The option ids are `allow` and `deny`. Oh My Pi's
own approval dialog passes its `Approve` / `Deny` through. A mode change
restarts the session. `Auto` is enforced by neither flavor and runs as Ask.

**Rollback** — `get_fork_messages` lists the active branch's user entries;
the live session forks (`branch` on Oh My Pi) at the first removed one and
`get_state` reads the replacement back (`replaced: true`). Rolling back to the
thread start opens a `new_session`. **Fork** never touches the live session:
a throwaway `pi --mode rpc --approve --fork <file>` (channel `fork-1`) copies
the source in the session's directory, forks the copy at the first removed
user entry, reports it with `get_state` and exits. Both count user entries
from the end, because Pi's events carry no turn id (turn ids bind by ordinal).

**Usage limits** — `errorStatus: 429`, or Oh My Pi's `errorId` UsageLimit
flag (`0x80000`), on the failed message or the retry notice; `reset_at` is
the end of the wait the agent's own retry announced (`auto_retry_start.delayMs`).
Never parsed from text.

**Gaps** — images on `prompt` (#78), `compact` (#76), MCP through a generated
extension (#84), Computer Use (Waku's Pi extension needs a driver-event
channel the orchestrator does not carry yet), the slash-command catalog
(`get_commands`, `available_commands_update`), and subagents. Capabilities
are in [capabilities_pi.json](../crates/waku-core/fixtures/capabilities_pi.json)
and [capabilities_ohmypi.json](../crates/waku-core/fixtures/capabilities_ohmypi.json).

**Models** — a separate `pi --mode rpc --no-session --no-skills
--no-prompt-templates --no-context-files` process answering
`get_available_models` and `get_state`. Extensions stay enabled because they can
register model providers. Ids are `provider/model` slugs; one that is not is
left to the agent's own default.

Oh My Pi rejects unknown flags outright, so its probe is its own list —
`--no-session --no-skills --no-rules --no-extensions` — and the two describe
thinking differently. Pi maps levels through a per-model `thinkingLevelMap`; Oh
My Pi advertises the levels a model actually honors under `thinking.efforts`.
`off` never appears in that list because it bypasses provider mapping entirely,
yet it is always accepted, so it is added back
([model_catalog.rs](../crates/waku-core/src/model_catalog.rs)).

**Replay** — `crates/waku-core/fixtures/replay/<scenario>/pi.ndjson` and
`ohmypi.ndjson`; their sources are in
[fixtures/replay/README.md](../crates/waku-core/fixtures/replay/README.md).

---

## Claude Code

**Launch** — `claude -p --input-format stream-json --output-format stream-json
--verbose --include-partial-messages --replay-user-messages
--permission-prompt-tool stdio --permission-mode <mode>`
([driver/claude/](../crates/waku-core/src/driver/claude/mod.rs)), plus `--model`, `--effort`,
and `--session-id` or `--resume`.

This is the transport the Claude Agent SDK's `query()` drives; the SDK is a
wrapper around these flags, not a separate capability, and there is no Rust SDK
to use instead. Both `--input-format stream-json` and `--permission-prompt-tool`
were verified against the real binary — **the latter is undocumented and absent
from `claude --help`**, and without it the CLI decides permissions itself and
only reports denials after the fact on `result`.

**`DriverV2` (#47)** — the driver is a `DriverV2` session actor
(`mod.rs`), with the frames in `protocol.rs` and the stream state machine in
`normalize.rs`, driven by the `ProviderSessionManager`. `ensure_thread` only
picks the `--session-id`; the process starts
with the first turn (so `ContextKind::System` blocks of that turn ride
`--append-system-prompt`) and a turn starts at its echoed prompt. Capabilities
are fixed (`fixtures/capabilities_claude.json`); the replay fixtures under
`fixtures/replay/*/claude.ndjson` are hand-authored (no live recording) and
their sources are in `fixtures/replay/README.md`.

| Capability | Claude |
| --- | --- |
| Turn ids | weak: the uuid of the assistant message that ended the turn |
| Steering | native, folded at the next model call; running tools are **not** cancelled (`active_steering_interrupts_tools = false`) |
| Rollback, fork | yes, by copying the native transcript (`replaced: true`); native `--fork-session` / `--resume-session-at` are present in CLI 2.1.304 but unproven, so unused |
| Runtime-mode switch in session | no: `set_permission_mode` exists in the CLI's control schema but is unproven, so a mode change restarts the session |
| Usage limits | `rate_limit_event` `rejected` (reset from `resetsAt`, epoch seconds) or an assistant `error: "rate_limit"` / `api_error_status: 429` fail the turn as `UsageLimit` |
| Subagents | `Agent`/`Task` tool calls become subagents with one child turn each; `stop_task` stops them and detached tasks |
| Plans | `TodoWrite` → todo list, `ExitPlanMode` → proposed plan |

**Lifetime** — long-lived. One process serves the conversation, with turns fed
as newline-delimited user messages on stdin.

**Per turn** — write `{"type":"user","message":{"role":"user","content":[…]},
"parent_tool_use_id":null}`; the turn ends with a `result` message carrying
`is_error`, `stop_reason`, usage, and `permission_denials`.

**Inbound stream**

| Message | Becomes |
| --- | --- |
| `system` / `init` | the model, and `AvailableCommandsUpdated` |
| `stream_event` → `text_delta`, `thinking_delta` | coalesced `MessageUpdated` / reasoning `TurnItemUpdated` snapshots |
| `assistant` content blocks | `tool_use` → a tool `TurnItemUpdated`; text and thinking only as a fallback when no delta of that kind streamed |
| `user` with `tool_result` | completes the matching tool item |
| `user` with `isReplay: true` | Waku's own prompt echoed by `--replay-user-messages`: the turn starts here (`ProviderTurnUpdated`) |
| `result` | the turn's one `TurnTerminal` |
| `system` `task_started` / `task_progress` / `task_updated` / `task_notification` / `background_tasks_changed` | `SubagentUpdated` (with a child turn) and `BackgroundWorkUpdated` |
| `rate_limit_event` | usage-limit evidence for the turn's `result` |
| `system` status/thinking-token notices | ignored |

**Approvals** — `control_request` / `subtype: "can_use_tool"` carries the tool
name, input, `tool_use_id`, the `blocked_path` that tripped the check, and
`permission_suggestions`. Waku answers with a `control_response` whose result is
`{"behavior":"allow"}` or `{"behavior":"deny","message":…}`. Outside Supervised it
answers allow itself.

**Cancel** — a `control_request` with `subtype: "interrupt"`.

**Steer** — the same user-message write as a prompt, sent while a turn is
running and without arming a new turn. The CLI holds the message and folds it
into the running turn at its next model call — one `result` still settles the
whole exchange, and the `isReplay` echo arrives at the moment of absorption
rather than at write time. Verified against the real CLI by injecting an
instruction while a Bash `sleep` ran: the same turn's reply honored it. Amp
was probed the same way and behaves differently — see its section.

**Model changes** — a `control_request` with `subtype: "set_model"`, so switching
models keeps the session. The permission posture is a launch flag and still
restarts.

**Native checkpoints** — a turn's weak native id is the uuid of the assistant
message that ended it. When a turn carries none, the driver reads Claude's own
transcript at `$CLAUDE_CONFIG_DIR/projects/**/<session>.jsonl`, walks the
`parentUuid` chain to find the active branch, and takes the checkpoint for that
turn ordinal (`claude_session::message_id_for_turn_in`,
[claude_session.rs](../crates/waku-core/src/claude_session.rs)). That
checkpoint is what makes rewind and branch possible. Because Claude accepts a
caller-chosen `--session-id`, the cursor exists before the first turn does.

**Rewind and branch** — the driver's `rollback` and `fork` copy the JSONL
transcript into a *new* session file with `claude_session::fork_session_with`,
truncated at the last kept turn's checkpoint and re-keyed with fresh uuids; the
new provider thread's cursor carries the re-keyed checkpoint as `resume_at`.
Rolling back to the thread start opens an empty conversation under a fresh
session id. The CLI also exposes
`--fork-session` (with `--resume`) and the hidden `--resume-session-at`
(`requires --resume`, print mode only, per the 2.1.304 binary); `--session-id`
may be combined with `--resume` only together with `--fork-session`. None of
this is proven against a live run, so the `DriverV2` port keeps the
hand-rolled copy, now written with derived ids so a replayed fork is
deterministic.

**Models** — the sessionless SDK `initialize` control response publishes the
same account- and configuration-aware list used by `/model`, including custom
routes resolved through CC Switch. Waku probes it in the background and caches
the last successful catalog; the curated list is only the startup/failure
fallback ([model_catalog.rs](../crates/waku-core/src/model_catalog.rs)).

---

## Amp

The driver is on `DriverV2` (RFC 0002, #51):
[driver/amp/](../crates/waku-core/src/driver/amp/mod.rs), opened and driven by
the `ProviderSessionManager`.

**Threads** — `ensure_thread` runs `amp threads new` (a short-lived process on
channel `threads-new`) and binds the `T-…` id it prints. The session process
starts with the first turn:
`amp threads continue <id> --execute --stream-json-thinking
--stream-json-input --dangerously-allow-all [--mode M] [--effort E] [--fast]`.
`--stream-json-thinking` implies `--stream-json`, which `--stream-json-input`
requires. `resume_thread` does no I/O: the next turn continues the thread.

**Protocol** — newline-delimited JSON in both directions. Amp keeps the process
alive until *both* the assistant is done and stdin closes, which is what makes
one process serve the thread's turns. Turns are written as
`{"type":"user","message":{"role":"user","content":[…]}}`, with every context
block in the shared `WAKU_CONTEXT_V1` envelope.

**Turn completion is not a `result` message.** The turn is over when an
`assistant` message carries `stop_reason: "end_turn"`; a `tool_use` stop
reason is mid-turn. This was found by probing — a driver waiting for `result`
hangs forever. An error `result` fails the turn.

**Inbound stream** — Anthropic-shaped: `system`/`init` names the thread;
`assistant` frames are whole messages (text, thinking and `tool_use` blocks —
no partial deltas); `user` frames carry `tool_result`. Redacted thinking is
ignored, and a subagent's frames (`parent_tool_use_id` set) never reach the
parent's transcript.

**Access modes and approvals** — none on the stream: Amp's rules live in
`amp permissions`, so the port declares `approvals` all false,
`supported_modes = [full_access]` and client-boundary enforcement, and the
orchestrator refuses the other modes. Amp's "models" are agent modes (the
agent preset, else the model, becomes `--mode`); mode, effort and the fast
tier are launch arguments, so changing any of them restarts the session.

**Interrupt** — no stream interrupt exists. `interrupt_turn` sends SIGINT
through the transports (the one signal RFC 0002 allows; replay records it as
an outbound `signal` frame) and retires the process. The turn ends
`Interrupted` with a reusable thread, and the next turn starts a new process
with `threads continue`.

**Steer** — the user message with a documented top-level `"steer": true`
attribute. A plain mid-turn message is held until the current turn's
`end_turn` and then runs as a turn of its own; the attribute marks it for
handling at the next interruption point instead, so the running turn absorbs
it and one `end_turn` settles everything. Both behaviors probed against the
real CLI — the plain-message probe is why an unmarked write must never be
used as a steer.

**Branch and rewind** — not native (D-6). The orchestrator hands a fork's
history to a new thread and rolls back with a fresh thread plus a summary;
the driver's `fork` and `rollback` are `Unsupported`. `amp_session.rs` keeps
discovery (`list_provider_sessions`, for v8 `providerSessions.list`) and
`history_from_export`, which still expands branch prompts the v1 driver seeded
with `WAKU_AMP_BRANCH_CONTEXT_V1`; `read_snapshot` is `amp threads export <id>`
through it.

**Gaps** — approvals, an interrupt that keeps the process, model discovery,
the native title (it was polled from the `threads` index; titles come from
the orchestrator's title port now), images. Capabilities are in
[capabilities_amp.json](../crates/waku-core/fixtures/capabilities_amp.json);
fixtures are `crates/waku-core/fixtures/replay/<scenario>/amp.ndjson`.

---

## OpenCode background service

OpenCode 2 is the `opencode` command. Its 2.0 release replaced the
per-workspace `opencode serve` of OpenCode 1 with one background service per
user that serves every workspace, and the CLI, TUI and desktop app all share
it.

**Driver** — a `DriverV2` (RFC 0002 "OpenCode", #49):
[driver/opencode/](../crates/waku-core/src/driver/opencode/mod.rs). One actor
task per session on the driver runtime talks to the service through
`driver::v2::http` (pooled keep-alive HTTP/1.1, SSE). Turn ids are weak: the
inbox id the prompt answers, which is the id of the user message that opens
the turn. Capabilities are in
[capabilities_opencode.json](../crates/waku-core/src/driver/opencode/capabilities_opencode.json).

**Launch** — usually none. Waku adopts the service the user's own `opencode`
already started, found through its registration file
(`${XDG_STATE_HOME:-~/.local/state}/opencode/service.json`, opened read-only)
and confirmed with `GET /api/info`, whose `pid` and `version` must match the
registration
([opencode_service.rs](../crates/waku-core/src/opencode_service.rs)). Only
when none is registered does a session start run `opencode serve --service`,
which yields to a healthy incumbent, and poll the registration for up to 20 s.
An OpenCode 1 binary is refused before that spawn: its `serve` has no service
mode, would never register, and would be left running as an orphan.

**Protocol** — OpenCode's HTTP API, authenticated with HTTP Basic (`opencode`
and the registration's password), plus the server-sent event stream
`GET /api/event`, which carries every session the service runs — the user's own
terminal sessions included. Routes and payloads were checked against the
2.0.22 OpenAPI document and schema sources. Each session opens its own
downlink and keeps only the frames for its native session, by
`data.sessionID` (`form.created` by its nested session id); `tui.*`
remote-control events are dropped.

**Lifetime** — the service is never Waku's; see
[How the long-lived processes actually die](#how-the-long-lived-processes-actually-die).
A stream that breaks resubscribes (250 ms → 2 s backoff) rather than reporting
the session lost, never starting the service itself, then reconciles: it
re-lists pending permissions and forms, and settles a turn whose execution
ended while the stream was down from `GET /api/session/active` and the
session's `outcome`.

**Service restarts** — stopping the service mid-turn suspends the run rather
than ending it: the service keeps the run's claim and resumes it as it boots
again, with no `idle` marker in between, so OpenCode counts one turn. Waku
does the same: a `shutdown` interruption does not settle the turn, and the
resumed run streams into it. A turn the user stops while the service is down
cannot be interrupted, so its run is interrupted once it resumes instead of
appearing as a turn of its own.

**Handshake** — subscribe first, then `POST /api/session` without an id, with
`location.directory` (the canonical workspace path), the agent (`build` unless
a preset is chosen) and the model. The service mints the session id; the actor
reads the stream only after the create answers, so no event can race it. A
resume reads `GET /api/session/{id}` instead, and a session the service no
longer has starts a fresh one for the same provider thread.

**Per turn** — `POST /api/session/{id}/prompt` with `{text}`, answered with the
inbox entry it became. A registered `/command` goes to
`POST /api/session/{id}/command` with `{name, text}` instead, so the service
expands it itself.

**Steer** — the same prompt with `delivery: "steer"`; an entry the service
queued anyway is promoted with `PATCH /api/session/{id}/inbox/{inboxID}`. The
steered message joins the running execution, so its one outcome still settles
the turn.

**Inbound stream**
([driver/opencode/normalize.rs](../crates/waku-core/src/driver/opencode/normalize.rs)):

| Event | Becomes |
| --- | --- |
| `session.text.*`, `session.reasoning.*` | coalesced `MessageUpdated` / reasoning `TurnItemUpdated` snapshots; text and reasoning share one ordinal namespace per assistant message |
| `session.tool.input.started` through `session.tool.success` / `session.tool.failed` | a tool `TurnItemUpdated`, opened while the arguments stream and kept through `session.tool.progress` |
| `session.step.ended` | the turn's token usage (`ProviderTurnUpdated`), against `/api/model`'s context window |
| `session.execution.succeeded` / `failed` / `interrupted` | the turn's one `TurnTerminal` — the only settle point, since the service never publishes `session.idle`; a `shutdown` interruption suspends the turn instead |
| `permission.asked` | `RuntimeRequestUpdated` (approval) |
| `form.created` | `RuntimeRequestUpdated`, one question per form field |
| `session.shell.started` / `ended` | `BackgroundWorkUpdated` |
| `session.compaction.*`, `session.retry.scheduled` | one upserted `TurnItemUpdated` each (compaction, system notice) |
| `session.deleted` | a failed `TurnTerminal` that marks the thread broken |

**Approvals** — `POST /api/session/{id}/permission/{requestID}/reply` with
`{decision: "once" | "reject"}`. `always` never goes on the wire: it would
write a rule into `/api/permission/saved`, a global store shared with the
user's own OpenCode, so a durable choice stays in the driver and every reply is
one-shot. The auto modes answer `once`.

**Forms** — `POST /api/session/{id}/form/{formID}/reply` with an answer keyed
by each field's own `key`.

**Cancel** — `POST /api/session/{id}/interrupt`.

**Rewind and branch** — `POST /api/session/{id}/fork` with `{before}` naming
the user message that opens the first removed turn, or no `before` to copy the
whole history; a rollback replaces the provider thread with the fork. A user
message is not a turn: a steer is stored as a `user` message of its own, so
turns are delimited by the `idle` transcript messages the service writes
whenever an execution settles, OpenCode's own rule in `session/diff.ts`.
History imported from OpenCode 1 carries no markers; there a message joined
the running turn when the step before it handed back to tools, or had not
completed yet when the message was written. The driver's rollback, fork and
snapshot all count turns this way (`TurnBoundaries`,
[opencode_session.rs](../crates/waku-core/src/opencode_session.rs)).

**OpenCode 1 history** — OpenCode 2's first start imports OpenCode 1 sessions
into the same database under their original ids, in the background. A cursor
written for OpenCode 1 resumes as it is once the import has reached it; the
driver does not wait for the import.

**Cold paths** — the Resume picker (`list_provider_sessions`, serving v8
`providerSessions.list`) and the model and agent catalogs
(`discover_catalog`) attach to a running service but never start one, so with
no service running the model picker keeps its cached catalog.

**Gaps** — Computer Use is not ported (#49); auto titles from
`session.renamed` and slash-command discovery events are not forwarded; the
mid-turn gap-fill of text after a stream break is not ported.

---

## Agent Client Protocol

**Launch** — `cursor-agent acp`, `fx acp`, `grok agent [--reasoning-effort E] stdio`, `kimi acp`
([driver/acp/](../crates/waku-core/src/driver/acp/mod.rs)).

**Protocol** — newline-delimited JSON-RPC over stdio, bidirectional. One agent
process serves the whole conversation, streams `session/update` notifications,
and asks the client for tool permission with a real request it expects an answer
to. Alongside Codex's app-server, this is the only transport where Waku's
Supervised mode means what it says.

**Driver** — one `DriverV2` for every flavor (#48), the flavor taken from the
instance's provider; a spec-only ACP agent is the `acp` flavor. It speaks
JSON-RPC itself, with request ids from 1 per process, so replay fixtures
(`fixtures/replay/<scenario>/{acp,grok,cursor,kimi,fx}.ndjson`, sources in
`fixtures/replay/ACP_FIXTURES.md`) match literally; the SDK still serves the
session catalog. Capabilities are the RFC 0002 ACP column, with
`threads.read_snapshot` negotiated from `initialize`'s `loadSession`
(snapshots in `driver/acp/snapshots/`). Like every driver, it is opened and
driven by the `ProviderSessionManager`.

**Lifetime** — long-lived, like Codex and Pi. Cursor and Grok previously spawned
a process per turn; Fx and Kimi Code arrived on this transport directly.

**Handshake** — `initialize` (advertising **no** `fs` or `terminal` client
capability, since Waku does not proxy the agent's file or terminal access — an
advertised capability the client cannot honor strands the agent mid-tool-call;
Cursor alone receives its `_meta.parameterizedModelPicker` opt-in) →
`session/resume` when resuming and the agent advertises it (so history is not
replayed), otherwise a replay-suppressed `session/load` when it reports
`loadSession`, else `session/new` → optional `session/set_mode` for Fx's access
policy. A restore the agent no longer recognizes falls back to a fresh session
rather than stranding the task. Kimi Code advertises both, so it takes the first
rung — `session/resume`, verified against a session left by an earlier process.

Cursor's picker opt-in makes `session/new`, `session/load`, and
`session/resume` return provider-owned `configOptions`. Model discovery uses
the same opt-in: Waku calls `cursor/list_available_models` after `initialize`
and maps each model's config options onto the ordinary reasoning-effort,
service-tier (`fast`), and context-window pickers. The CLI's flat aliases
(`cursor-grok-4.6-xhigh-fast`) still resolve to the advertised base value,
then Waku applies `thought_level`/`effort`, `thinking`, `fast`, and `context`
from the session's traits (or from that alias suffix). If an older Cursor
agent advertises no model option, Waku retains the legacy `session/set_model`
request, and if ACP discovery is empty it falls back to parsing
`cursor-agent models`.

Fx also returns provider-owned config options, but its first model-category
option selects an account provider while the option whose id is `model` selects
the model. AI Gateway IDs such as `openai/gpt-5.6-luna-fast` are absent until
Waku first selects Fx's `gateway` provider option and reads the refreshed model
option from that response. Waku then targets the exact `model` id with
`session/set_config_option`; falling back to the older `session/set_model`
extension would not change Fx's model.

**Per turn** — `session/prompt`, whose response stays open until the turn ends.
It is tracked apart from the blocking request table precisely so the writer stays
free to send a cancel while it is outstanding; its reply is what emits the
turn's one `TurnTerminal`, keyed off `stopReason`.

**When `stopReason` lies.** Kimi Code answers a turn its model provider
rejected — an inactive plan, a spent quota — with a clean `end_turn` carrying no
content at all: no error, no JSON-RPC failure, nothing on stderr. Trusting the
protocol there shows the user an empty answer reported as a success, with no
cause to act on. The cause is recoverable, just not from the wire: Kimi appends
a `turn.ended` record with the real message to its own per-session log at
`<KIMI_CODE_HOME>/sessions/<workspace>/<session>/agents/main/wire.jsonl`.

[kimi_session.rs](../crates/waku-core/src/kimi_session.rs) reads it
(`turn_failure`), and the prompt's completion lets a recovered failure override
the protocol's verdict — the turn's terminal is `Failed` with the provider's
own wording. Three details make it safe:

- **It is scoped to a turn that produced nothing.** The turn's
  `produced_content` flag records whether any message, thought, tool call, or
  plan arrived. A turn that streamed anything is settled by `stopReason` alone
  and does no I/O.
- **It waits.** The record lands *after* the ACP response — roughly 50ms in
  practice — so an immediate read finds nothing. The lookup polls, bounded at
  one second, and gives up quietly.
- **It ignores earlier turns.** The log's byte length is captured before the
  prompt is sent, and only what is appended past that offset is scanned, so a
  previous turn's failure can never be reported as this one's.

The read runs on a blocking task off the driver runtime. `kimi_session`'s
tests cover the record lookup, the wait, and the deadline.

**Inbound stream** — `session/update` notifications:

| `sessionUpdate` | Becomes |
| --- | --- |
| `agent_message_chunk` | coalesced `MessageUpdated` snapshots |
| `agent_thought_chunk` | a reasoning `TurnItemUpdated` |
| `tool_call`, `tool_call_update` | a tool `TurnItemUpdated`, correlated by `toolCallId` |
| `plan` | `PlanUpdated` |
| `usage_update` | the turn's context usage (`ProviderTurnUpdated`), not transcript content |
| `available_commands_update` | `AvailableCommandsUpdated` — the composer's slash-command list |
| `session_info_update` | ignored by the driver; see [titles.md](titles.md) |
| `user_message_chunk` | ignored — Waku's own prompt echoed back |

Everything outside `session/update` on that channel is agent-private control
traffic (Grok emits a stream of `_x.ai/*` notifications) and never reaches the
transcript.

Fx emits its context-limit and skill-discovery diagnostics as ordinary
`agent_message_chunk` updates before the model starts. Their reserved
`[context]` and `skill discovery warning:` prefixes are provider notices rather
than assistant content, so Waku filters that prelude from the transcript.

**Approvals** — `session/request_permission` becomes a `RuntimeRequestUpdated`
approval whose options come straight from the agent, with `kind` (`allow_once`, `allow_always`,
`reject_once`, `reject_always`) deciding which read as allow. The detail line is
the agent's own explanation from `toolCall.content` ("Not in allowlist: cat,
pwd") rather than a sentence synthesized from the tool kind — that reason is the
whole basis for the user's decision. Outside Supervised, Waku answers for the
user and prefers the durable allow so the agent stops asking about the same tool.

**Why the client advertises no `fs` or `terminal` capability.** Those declare
services *Waku offers the agent*, not permissions the agent needs. `fs` exists so
an editor can serve unsaved buffer contents in place of what is on disk, and
`terminal` lets the agent run commands through the client's own terminal. Waku
provides neither, so the agent uses its own read and shell tools and reaches the
filesystem exactly as before — verified against `cursor-agent acp` with both
declined: it read a file, ran a shell command, and ended the turn normally.
Advertising a capability Waku cannot service is the harmful choice, because the
agent would call `fs/read_text_file` and wait forever for a reply.

T3 Code lands in the same place: its `AcpSessionRuntime` defaults to
`fs.readTextFile: false`, `fs.writeTextFile: false`, `terminal: false`, Grok
passes no override, and Cursor's is only `_meta.parameterizedModelPicker`. The
handler registration points in its `packages/effect-acp` belong to a
general-purpose ACP library, not to the app that drives these two providers.

The one case that would justify serving `fs/read_text_file` is Waku's own file
editor, which tracks unsaved buffers
([src/app/right_panel.rs:1004](../src/app/right_panel.rs#L1004)): an agent
reading a file the user has unsaved edits in currently gets the disk copy. That
is a deliberate future call, not an oversight.

**Access modes** — Fx exposes native `ask` and `code` modes, so Waku maps
Supervised to `ask` and the auto modes to `code`. Every other ACP agent stays in
its ordinary execution mode, and `auto_approve` decides whether Waku answers
`session/request_permission` on the user's behalf. That is why Kimi is left in
`default` rather than switched to `auto` or `yolo`: the permission traffic is
the feature, not an obstacle. A legacy session still reporting the removed
read-only setting is returned to the agent's advertised `agent` or `default`
mode when it attaches.

**Model and reasoning effort** — `session/set_model` after the session opens,
then the effort as a session config option. **The config id is the agent's to
name**, and the two disagree: Waku sends `mode` by default, but Kimi's `mode` is
its permission mode and its effort lives on
`thinking`. `reasoning_effort_config_id` resolves that per provider — sending
the default id to Kimi would silently set nothing, or worse, move the permission
mode. The call is non-fatal either way, since an agent may expose no effort at
all. Grok is the exception: effort rides on `session/set_model` as
`_meta.reasoningEffort` (and as `--reasoning-effort` at launch), not as a
session config option.

Cursor is the other exception. Its parameterized picker exposes effort, fast
mode, thinking, and context as per-model `configOptions` rather than a single
well-known id. Discovery reads those options from `cursor/list_available_models`;
the live session applies them with `session/set_config_option` after selecting
the base model. Fast is Waku's service-tier control (`fast` vs Standard).
Thinking is not a separate picker: it turns on when a non-`none` effort is
applied, because that is what reveals the thought-level option.

Grok's catalog comes from the plain-text `grok models` listing, which reports
ids but no effort metadata. The hardcoded menu therefore covers only the exact
built-ins (`grok-4.5` stops at high, `grok-4.6` offers xhigh): the listing also
includes custom models from the user's config, whose effort support the id
alone cannot establish, so they are offered without an effort menu. Discovery
is authoritative — a stale fallback would name a model the CLI rejects.

Kimi's catalog comes from `kimi provider list --json`, which covers both the
managed plan and any registry the user imported with `kimi provider add`. Only
the K3 family reports `supportEfforts`; the rest expose a single always-on
thinking state, which is not a user choice and so is not offered as one. The
JSON omits the configured default, so the plain-text listing supplies that one
field — hence two probes
([model_catalog.rs](../crates/waku-core/src/model_catalog.rs)).

**Cancel** — `session/cancel`, a notification; the open `session/prompt` reports
the cancellation.

**Steer** — no native steering (RFC 0002 D-7, #48). The v1 driver steered by
sending a second `session/prompt` that superseded the open one; that is outside
the ACP spec and was never probed for Kimi Code, so the `DriverV2` port removed
it. Every flavor declares `turns.active_steering = false` and
`turns.steering_by_interrupt_restart = true`, and `steer_turn` answers
`Unsupported`. The orchestrator steers by interrupt and restart:
`session/cancel`, wait for the `cancelled` stop reason, then a new
`session/prompt` as a `steering_restart` attempt of the same run (replay
scenario `steering_restart`).

**Rewind and branch** — Grok only, by replacement: the driver asks a
short-lived second Grok process (replay channel `fork-1`) for
`_x.ai/session/fork`, truncates the copy's `chat_history.jsonl` and
`updates.jsonl` to the retained prompts, and returns the copy as a new provider
thread (`RollbackOutcome { replaced: true }`); the next turn resumes it with
`session/resume` ([driver/acp/grok.rs](../crates/waku-core/src/driver/acp/grok.rs)).
The fork process is closed by its stdin once it has answered. Every other
flavor declares no fork or rollback: for Cursor, the orchestrator's portable
handoff (D-6) replaces the v1 driver's re-seeding of a branch with its retained
history.

**Kimi Code and Fx have neither, deliberately.** Kimi advertises a `fork` session
capability, but `session/fork` takes only `{sessionId, cwd}` and copies the
whole conversation — there is no turn count, so "drop the last N turns" cannot
be expressed. Fx exposes no turn-aware fork or truncation method.
`ProviderKind::supports_conversation_fork` and
`supports_conversation_rollback` are therefore false for both, which hides the
rewind and branch affordances rather than offering a control that would silently
keep history the user asked to discard. Restoring these depends on Kimi
accepting a truncation point.

**Computer Use** — not wired. Grok's isolated `GROK_HOME` and `--rules` setup
existed only behind the v1 `V1Bridge` and was removed with it; the ACP driver
launches no Computer Use setup.

**What moving to ACP gained.** Grok's Supervised mode no longer means "deny"
(`--permission-mode dontAsk` existed because the one-shot stream had no response
channel), Cursor's no longer means `--force`, and **Cursor streams reasoning**,
which its `--print` transport did not emit at all.

---

## Access modes across providers

Waku's `RuntimeMode` (Supervised / Auto-accept edits / Auto / Full access)
maps into each CLI's own vocabulary.

| Waku | Codex (`approvalPolicy` / `sandbox` / reviewer) | Claude `--permission-mode` | Cursor | Fx | OpenCode | Grok | Kimi Code |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Supervised | `untrusted` / `read-only` / `user` | `default` + `can_use_tool` reaches the user | `session/request_permission` reaches the user | `session/set_mode` → `ask` | permission requests reach the user | `session/request_permission` reaches the user | `session/request_permission` reaches the user |
| Auto-accept edits | `on-request` / `workspace-write` / `user` | `acceptEdits` | auto-answered | `session/set_mode` → `code` | edits auto-answered (`once`), the rest reach the user | auto-answered | auto-answered |
| Auto | `on-request` / `workspace-write` / `auto_review` | `auto` | auto-answered | `session/set_mode` → `code` | auto-answered (`once`) | auto-answered | auto-answered |
| Full access | `never` / `danger-full-access` / `user` | `bypassPermissions` + `--dangerously-skip-permissions` | auto-answered | `session/set_mode` → `code` | auto-answered (`once`) | auto-answered | auto-answered |

Amp accepts Full access only and always runs wide open
(`--dangerously-allow-all`): its stream carries no permission request, so
its `DriverV2` port declares `supported_modes = [full_access]` with
client-boundary enforcement and the orchestrator refuses the other modes
(#51).

Every other provider distinguishes Supervised from the auto modes in a way the
user can actually answer. Pi has no permission system of its own, so since #50
a supervised Pi session loads Waku's extension, whose `tool_call` hook asks
through a `confirm` dialog; Oh My Pi runs with its own `--approval-mode`
instead of `--yolo` and asks through a `select`. Neither enforces `Auto`, which
runs as Supervised.

## Resume cursors

`ProviderResumeCursor` ([model.rs](../crates/waku-protocol/src/model.rs)) is
persisted with the session and is what makes a Waku task outlive its process:

| Provider | Cursor fields | Why |
| --- | --- | --- |
| Codex | `thread_id` | `thread/resume` |
| Pi | `session_id`, `session_file` | `switch_session` needs the path |
| Oh My Pi | `session_id`, `session_file` | same, plus `--fork <file>` for a whole-session copy |
| Claude | `session_id`, `resume_at` | `resume_at` is the transcript message uuid used for forking |
| Amp | `thread_id`, `fork_context` | `threads continue`; `fork_context` (the v1 driver's seeded branch history) is no longer written |
| Cursor | `session_id`, `fork_context` | `session/resume`; `fork_context` is no longer written — branches are the orchestrator's portable handoff |
| Fx | `session_id` | `session/resume`; no fork or rewind, see above |
| OpenCode | `session_id`, `directory` | `GET /api/session/{id}` / service fork; `directory` is the workspace string the service stored |
| Grok | `session_id` | `session/resume` / `_x.ai/session/fork` |
| Kimi Code | `session_id` | `session/resume`; no fork, see above |

A cursor from the wrong provider is rejected at driver start rather than
silently ignored.

## Compared with T3 Code

[T3 Code](https://github.com/pingdotgg/t3code) solves the same problem with five
drivers — `codex`, `claudeAgent`, `cursor`, `grok`, `opencode` (no Amp, no Pi) —
registered in `apps/server/src/provider/builtInDrivers.ts` and documented in its
own `docs/internals/providers.md`.

**Its one structural difference: no provider is a per-turn process.** All five
hold a long-lived session; the transport differs, the lifetime does not.

| Provider | T3 Code transport | Waku transport |
| --- | --- | --- |
| Codex | `codex app-server` JSON-RPC (`packages/effect-codex-app-server`) | same |
| Claude | `@anthropic-ai/claude-agent-sdk` `query()` with an `AsyncIterable` prompt queue | same protocol, spoken directly — the SDK is a wrapper around these flags |
| Cursor | **`cursor-agent acp`** — ACP over stdio (`packages/effect-acp`) | same |
| Grok | **`grok agent stdio`** — ACP over stdio | same |
| OpenCode | long-lived `opencode serve` + HTTP SDK | the user's shared OpenCode 2 background service, adopted rather than started per workspace |

**All five now hold a long-lived session**, and Claude reaches the same place
without the SDK: there is no Rust Agent SDK, but the SDK is a wrapper around the
`claude` CLI's own streaming-input protocol, which Waku speaks directly. No Node
sidecar and no npm dependency.

Waku goes one further than the comparison: Amp and Pi, which T3 Code does not
support, are long-lived here too. Every provider holds a session.

What the long-lived session buys, and what Waku pays for not having it:

| Capability | T3 Code | Waku |
| --- | --- | --- |
| Interactive approvals | Every provider: Claude via the SDK's `canUseTool` (including `AskUserQuestion` and `ExitPlanMode`), Cursor/Grok via ACP `session/request_permission`, Codex via `*requestApproval*` | Every provider except Amp, which exposes no request to answer (Pi through Waku's extension since #50) |
| Interrupt | `session/cancel`, `query.interrupt()` (plus `stopTask()` for runaway subagents) | Protocol interrupt everywhere except Amp, which has none: its process gets SIGINT and is retired |
| Change model mid-session | `capabilities.sessionModelSwitch: "in-session"` → `session/set_model`, `query.setModel()` | Every transport keeps the session except Amp, whose mode is a launch argument |
| Mid-turn prompt | Queued into the live agent loop as a **steer**, same turn | `steer_turn` into the live turn where the session declares `turns.active_steering`; the ACP agents are interrupted and restarted within the same run |
| Native rollback | `rollbackThread` on the adapter contract | `DriverSession::rollback` where the session declares `threads.rollback` (Codex, Pi, Claude, OpenCode, Grok, DeepSeek); otherwise the orchestrator starts a fresh provider thread with a checkpoint-summary handoff |
| Idle cleanup | `ProviderSessionReaper` stops sessions idle 30 min, swept every 5 min, skipping threads with an active turn | `ProviderSessionManager::release_idle`: 30 min without a turn (up to 4 h while background work runs), swept every minute, skipping sessions with a running turn |

The T3 adapter contract is `startSession` / `sendTurn` / `interruptTurn` /
`respondToRequest` / `respondToUserInput` / `stopSession` / `listSessions` /
`hasSession` / `readThread` / `rollbackThread` / `stopAll` / `streamEvents`,
plus a declared `capabilities` record. Waku's `DriverV2` / `DriverSession`
pair covers the same surface (`open_session`, `start_turn`, `interrupt_turn`,
`respond`, `close`, `read_snapshot`, `rollback`, `fork`, the adapter-event
channel) and declares its capabilities per session, so the orchestrator's
policies read what a session can do rather than the provider's name. Session
discovery for the Resume picker stays outside the driver, in each
`*_session.rs`'s `list_provider_sessions` (v8 `providerSessions.list`).

Note the parts that are *not* a gap. Waku's Codex path is the same app-server
protocol against the same methods. Both projects normalize provider events into
one canonical activity/event stream that the UI consumes provider-agnostically.
Both keep a per-session resume cursor and both had to special-case Claude's
transcript uuid as a rewind checkpoint.

## Adding a provider

1. Add the variant to `ProviderKind`
   ([model.rs](../crates/waku-protocol/src/model.rs)) with `id`,
   `display_name`, `short_name`, `command`, and the capability predicates. The
   compiler's non-exhaustive-match errors are the reliable to-do list for
   everything that follows.
2. Add a `ProviderResumeCursor` variant carrying whatever resume actually needs
   (an id is often not enough — see Pi's session file and Claude's message uuid).
3. Pick a transport, and look hard before settling for the one-shot path. Ask
   whether the CLI speaks ACP (`acp` / `agent stdio` — [driver/acp/](../crates/waku-core/src/driver/acp/mod.rs)
   already covers it), serves an HTTP API, or has a persistent RPC mode; three
   providers were on a per-turn headless transport until someone checked, and
   none is left. Implement `DriverV2` for the transport (or add a flavor to an
   existing family), spawn and connect only through `ctx.transports`, and
   register it in `driver_for`
   ([daemon_host.rs](../crates/waku-core/src/daemon_host.rs)).
4. Map its stream onto `AdapterEvent`s and its tools onto `ActivityKind`
   (through `driver/tool_items.rs`), and declare its `Capabilities` and its
   `plan_selection_transition`. **Read
   the payloads off a live provider** — every driver here was written from a
   probe transcript or an OpenAPI document, and the two bugs that reached code
   anyway (a dead event subscription, a discarded permission reason) were both
   caught by running a real turn rather than by unit tests. Preserve ordering,
   and never leak private control markers into the transcript. If the transport
   accepts user messages mid-turn, probe *which* behavior it has before
   declaring `turns.active_steering`: inject an instruction while a slow tool
   runs and count the turn completions. Claude folds a plain message into the
   running turn; OpenCode joins it to the running execution when the prompt says
   `delivery: "steer"`; Amp queues it unless it carries the CLI's
   `"steer": true` attribute — and only a live probe tells these apart. Without
   a proven steer, declare `turns.steering_by_interrupt_restart` and let the
   orchestrator interrupt and restart, as the ACP agents do.
5. Map the access modes. If the transport can ask the user, route
   Supervised to a real `RuntimeRequestUpdated` approval; if it cannot, declare
   the modes it enforces (`runtime_policy.supported_modes`) and let the
   orchestrator degrade or refuse the rest, as Amp does.
6. Record replay fixtures for the RFC 0002 scenarios
   (`crates/waku-core/fixtures/replay/<scenario>/<provider>.ndjson`) from a
   live provider, and add an `#[ignore]`d test that drives the real binary. A
   live run is the only check that catches a transport wired to nothing.
7. Implement rewind and branch natively where the provider can, as Codex, Pi,
   Claude, OpenCode and Grok do. Native truncation is preferable; without it,
   declare `threads.fork` and `threads.rollback` false and the orchestrator
   falls back to a portable handoff, as for Amp and Cursor. Where even a
   handoff would misrepresent the history — Kimi's fork takes no turn count —
   answer the `ProviderKind` predicates with false and let the UI hide the
   affordance. A control that silently keeps history the user
   asked to discard is worse than one that is not there.
8. **Do not trust a clean stop reason.** Probe what the provider does when the
   turn cannot run at all: an expired plan, a spent quota, a rejected key. Kimi
   reports `end_turn` with no content and no error, and the real message is only
   in its own session log — a client that believes the protocol shows an empty
   answer and calls it a success. Where the cause is recoverable, recover it;
   where it is not, at least do not report success for a turn that produced
   nothing.
9. Wire model discovery in `model_catalog.rs`, plus a fallback list for when the
   binary is missing or the command fails. Some transports hand you a better
   catalog than the CLI's `models` output — Cursor and Grok both return one in
   their ACP handshake.

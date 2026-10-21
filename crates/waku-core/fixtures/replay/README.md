# Replay fixtures

One directory per scenario (RFC 0002 "Replay fixtures"): `input.json` holds the
provider-neutral steps, `<provider>.ndjson` one transcript per provider, an
optional `input.<provider>.json` replaces the steps where a provider needs its
own values (native question ids, model names, a runtime mode), and an optional
`<provider>.home/` seeds the provider's home directory. `tests/replay.rs` replays
every file it finds.

- `fake.ndjson` files are recorded with `waku-replay-record --provider fake`
  against the in-repo fake agent.
- `claude.ndjson` files (#47) are **hand-authored**. No live Claude Code run was
  recorded: they follow the stream-json protocol as Waku's driver and the
  bench daemon's `fake-claude` speak it, with the frame schemas CLI 2.1.304
  embeds for its SDK. Every header says `"recordedBy": "hand-authored"`,
  `"synthetic": true` and its `source`. Re-record them with
  `waku-replay-record --provider claude --scenario <name>` once a live run is
  allowed. Where their prompts differ from the shared `input.json` (written by
  the ACP port for `tool_call_read_only`, `turn_interrupt_mid_tool` and
  `runtime_exit_unexpected`), the steps are in `input.claude.json`.
- `opencode.ndjson` (#49) and `deepseek.ndjson` (#52) files are
  **hand-derived** (`synthetic: true`); see their sections below. Their
  prompts that differ from a shared `input.json` written by another port are
  in `input.opencode.json` / `input.deepseek.json`.

`tests/replay.rs` replays every transcript except the ACP family's (fake,
Codex, Claude, OpenCode, DeepSeek) through the real orchestrator and session
manager, and `replay_is_deterministic` replays each one twice; the ACP
family's are replayed by `tests/replay_acp.rs` (see `ACP_FIXTURES.md`), and
Codex's sources are in `CODEX.md`.

## Claude scenarios and their sources

| Scenario | Source of the frames |
| --- | --- |
| `simple` | Launch flags of the v1 driver; `--replay-user-messages` echo; partial `stream_event` text and thinking deltas; per-block `assistant` snapshots; `result` success (as `fake-claude` and the v1 driver's live-payload tests show). |
| `multi_turn` | Two prompts on one streaming process (`fake-claude`, the v1 driver's live two-turn test). |
| `tool_call_read_only` | A Bash `tool_use` block and its `tool_result` user frame (shapes from the 2.1.223 payloads quoted by the v1 driver's tests). |
| `tool_call_approval` | Supervised (`--permission-mode default`): a `can_use_tool` control request (shape observed from a live `--permission-prompt-tool stdio` session, quoted by the v1 driver's tests) answered `{"behavior":"allow"}`. |
| `plan_questions` | `TodoWrite` and `ExitPlanMode` tool calls, then an `AskUserQuestion` `can_use_tool` request answered with `updatedInput.answers` keyed by question text (the v1 driver's verified reply). |
| `turn_interrupt` | An `interrupt` control request mid-stream, acknowledged, then the `error_during_execution` / "Request interrupted by user" result `fake-claude` and the 2.1.304 schema use. |
| `turn_interrupt_mid_tool` | The same interrupt while a Bash tool runs, with no `tool_result` before the settling result. |
| `message_steering` | The v1 driver's live steering probe (2.1.223): a mid-turn user message is echoed when absorbed and folded into the same turn; one `result`. |
| `provider_thread_resume` | Unload closes stdin; the next turn starts a new process with `--resume`. |
| `thread_rollback` | `claude.home/` seeds the native transcript; rollback copies it up to turn one's assistant uuid (`claude_session::fork_session_with`, the app's existing path) and the next turn resumes the copy. |
| `thread_fork_native` | Same seed; the fork's thread resumes the copy in its own process. |
| `usage_limit` | Synthetic from the 2.1.304 schemas: `rate_limit_event` (`status: "rejected"`, `resetsAt` epoch seconds), an assistant `error: "rate_limit"`, a `result` with `api_error_status: 429`. |
| `runtime_exit_unexpected` | The process exits with status 1 mid-turn. |
| `background_work` | 2.1.258 shapes quoted by the v1 driver's tests: a backgrounded Bash task joins `background_tasks_changed` before `task_started`, the reply parks the turn; the settle arrives during the next turn (the prompt-less wake is unit-tested: in replay it would race the first turn's finalization). |
| `subagent` | 2.1.226 `task_*` shapes quoted by the v1 driver's tests: an `Agent` tool call, `task_started` (`local_agent`), the subagent's own frames (`parent_tool_use_id` set), `task_progress`, `task_notification` with the report. |
| `model_switch` | `set_model` control request on the live process (verified against 2.1.228 by the v1 driver). |

## `opencode` (OpenCode 2, `opencode.http-sse`, #49)

No transcript was recorded against a real `opencode serve`: the port was done
without running the CLI against the owner's accounts. Every frame was written
from the protocol as Waku already exercises it:

- request routes, bodies and envelopes from the v1 driver
  (`driver/opencode.rs` before #49) and the wire tests in `opencode_api.rs`
  (`turn_control_requests_use_the_released_routes_and_bodies`,
  `session_info_decodes_a_live_payload`,
  `inbox_user_reads_the_released_prompt_answer`, the catalogue and pagination
  tests), all verified against OpenCode 2.0.22 when they were written;
- `/api/event` envelope shapes (`session.execution.*`, `session.step.*`,
  `session.text.*`, `session.tool.*`, `permission.asked` / `replied`,
  `form.created` / `replied`, `session.inbox.*`, `session.shell.*`) from the
  v1 driver's decoder and its tests, and from T3's OpenCode 2 adapter
  (`packages/provider-opencode/src/server/v2/adapter.ts`) at the pinned
  commit;
- the usage-limit error types (`provider.quota`, `provider.rate-limit`) from
  T3's adapter. OpenCode reports no structured reset time, so `reset_at`
  stays unset.

Ids (`ses_1`, `msg_1`, `per_1`, …) are placeholders in OpenCode's shapes;
`<workspace>` is the harness workspace. The `stream_resync` scenario replaces
`runtime_exit_unexpected` for OpenCode (RFC 0002): the event stream closes and
is reopened, and the session is not lost. Re-record these with
`waku-replay-record --provider opencode --service-url http://opencode:<password>@127.0.0.1:<port> --cli-version <v> --scenario <s>`
once a live smoke test is possible; the recorder keeps a file only if it
replays green.

## `deepseek` (DeepSeek Harness, `deepseek.dsh-web`, #52)

Also written by hand, without running `dsh` against the owner's account:

- RPC routes and envelopes (`POST /api/<method>` with `client-request`,
  `server-response` results, `POST /api/respond` with `client-response`),
  the payloads of `session.create`, `session.history`, `session.prompt`
  (`mode: queue | steer`), `session.cancel`, `session.fork { atSeq }`,
  `commands/list` and `commands/execute`, and the `session.create` echo of
  the requested id, from the v1 driver (`driver/deepseek.rs` before #52) and
  `deepseek_session.rs`, including its ignored live test
  `installed_harness_session_contracts_round_trip`;
- downlink envelopes (`session/event` with `{seq, type, data}`,
  `approval/requested` / `resolved`, `question/requested` / `resolved`,
  `session/jobs`, `session/projection`) and event types (`turn/start`,
  `turn/end` with `reason.kind`, `assistant/chunk`, `assistant/message`,
  `tool/call`, `tool/result`, `todo/write`) from the v1 driver's decoder.

Not verified anywhere and marked as such in the files' `source`:
`usage_limit`'s error shape (`reason.error.code = "rate_limit_exceeded"`,
`resetAt` in epoch ms) and the `session/jobs` entry for a subagent
(`kind: "subagent"`). The session id is matched as `<any>` in
`session.create` (the driver mints a UUID) and every `rpcId` as `waku-<any>`
(a process-wide counter). `runtime_exit_unexpected` closes the mux downlink
of a Host given by URL, which the driver treats as the Host exiting.

# Codex replay fixtures (#46)

Every `<scenario>/codex.ndjson` here was derived without running the real
`codex` CLI. Their **inbound** frames are real Codex app-server frames recorded
by T3 Code (`t3code` at `a71a6c37d1`, the commit in `docs/fork/T3_PIN`,
`apps/server/src/orchestration-v2/testkit/fixtures/<name>/codex_transcript.ndjson`,
Codex 0.156.1 unless noted). Their **outbound** frames are what Waku's driver
actually writes: each fixture was produced by `waku-replay-record --provider codex
--script <adapted transcript>`, which plays the transcript's inbound frames to
the real driver through `driver::codex::script::ScriptedAppServer` (outbound
frames matched by method, request ids mapped onto the driver's own), records
both directions, runs the result through the replay sanitizer (UUIDs renumbered,
paths and secrets rewritten, fail-closed scan), and keeps it only if it replays
green. Headers therefore say `"synthetic": true` with the source in `metadata.source`.

Adapting a T3 transcript meant: splitting its consecutive processes into the
channels `main`, `main#2`; replacing T3's checkout path with `<workspace>`; and
dropping the requests Waku does not send (with their responses), as listed
below. Scenario inputs that differ from the shared `input.json` (T3's prompts,
Codex's native option ids, the runtime mode) are in `input.codex.json`.

| Scenario | RFC row | Source | Adaptation |
|---|---|---|---|
| `simple` | R | `simple` | none |
| `multi_turn` | R | `multi_turn` | none |
| `tool_call_read_only` | R | `command_output_streaming` (Codex 0.157.0) | none: a foreground command whose output streams as `outputDelta`s |
| `tool_call_approval` | S | `tool_call_read_only_on_request` | none; runs in `ask` mode and answers `accept` from the request's `availableDecisions` |
| `plan_questions` | S | `plan_questions` | none; answers `schema_preference` |
| `todo_list` | (extra) | `todo_list` | none: two `turn/plan/updated` |
| `turn_interrupt` | R | `turn_interrupt` | none |
| `turn_interrupt_mid_tool` | R | `turn_interrupt_mid_tool` | T3's follow-up `thread/backgroundTerminals/terminate` dropped |
| `message_steering` | S | `message_steering` | none |
| `provider_thread_resume` | R | `provider_thread_resume` | none (second process is `main#2`) |
| `thread_rollback` | S | `thread_rollback` | T3's `thread/read` history probe dropped; `thread/turns/list` + `thread/revert` kept |
| `thread_fork_native` | S | `thread_fork_native_continue` | T3 continues the fork on the same app-server; Waku runs it in a new session, so `main#2` gets the transcript's own `initialize` exchange and a `thread/resume` answered with the `thread/fork` result's thread; a `thread/unsubscribe` exchange (response `{"status":"unsubscribed"}`, as the bench `fake-codex` answers) follows the fork |
| `subagent` | S | `subagent_v2` | none: multi-agent v2 `subAgentActivity` items and the child thread's own turn |
| `usage_limit` | R | `simple` cut after `turn/started` | **synthetic tail**: `account/rateLimits/updated` (primary window 100 %, `resetsAt` 2000100000), an `error` notification and a failed `turn/completed`, both with `codexErrorInfo: "usageLimitExceeded"`; shapes from T3's `CodexAdapterV2.test.ts` usage-limit cases and the Codex 0.159.2 `ErrorNotification` / `TurnCompletedNotification` schemas |
| `runtime_exit_unexpected` | R | `simple` cut after the first `item/agentMessage/delta` | **synthetic tail**: the process exits with status 1 |
| `background_work` | S | `todo_list` | **reordered**: the command's `item/completed` moved after `turn/completed`, so the command outlives its turn as a background terminal |
| `daemon_restart` | (#59) | `provider_thread_resume` | **cut and continued**: the first process is killed (`daemon_restart`) after three answer deltas; `main#2` resumes the thread and its turn is the restart continuation ("Continue where you left off.", one answer delta) |

`steering_restart` is not applicable to Codex (it declares native steering).

To re-derive a fixture after a driver change, use its committed
`codex.ndjson` as the script:

```text
cargo run -p waku-core --bin waku-replay-record -- --provider codex \
  --scenario simple --script crates/waku-core/fixtures/replay/simple/codex.ndjson \
  --source "<the source column above>"
```

A live recording (`--provider codex` without `--script`) runs the real CLI on
the recorder's account; none of these fixtures was made that way.

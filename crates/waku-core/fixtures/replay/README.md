# Replay fixtures

One directory per scenario (RFC 0002 "Replay fixtures"): `input.json` holds
the provider-neutral steps, `input.<provider>.json` overrides them where a
provider needs its own native option ids or runtime mode, and
`<provider>.ndjson` is one transcript per provider. `tests/replay.rs` replays
every transcript through the real orchestrator and session manager, and
`replay_is_deterministic` replays each one twice.

## Sources

| Provider | Origin | Notes |
|---|---|---|
| `fake` | Recorded with `waku-replay-record --provider fake` against the in-repo fake agent. | `synthetic: false`. |
| `opencode` | **Hand-derived, no live recording** (`synthetic: true`). | See below. |
| `deepseek` | **Hand-derived, no live recording** (`synthetic: true`). | See below. |

### `opencode` (OpenCode 2, `opencode.http-sse`, #49)

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

### `deepseek` (DeepSeek Harness, `deepseek.dsh-web`, #52)

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

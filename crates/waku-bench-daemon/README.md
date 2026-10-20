# waku-bench-daemon

Deterministic benchmark fixtures for Waku (`docs/fork/audit/waku-perf.md` §4.2).
The crate is in the workspace but not in `default-members`, so ordinary
`cargo build` / `cargo test` skip it; name it with `-p waku-bench-daemon`.

## The bench daemon (#7)

`waku-bench-daemon` stands in for `waku-daemon`. The desktop cannot tell the two
apart on the wire, because the bench daemon runs the same code:

- the server is `waku_core::serve`, so the handshake, token check, protocol
  version, event sequencing, replay journal and back-pressure are the real ones;
- every command that does not reach a provider goes to the real `WakuBackend`
  over a throwaway SQLite database. That covers task state, hydration, saves,
  drafts, blobs, search, settings, workspace and terminal.

Only the provider runtime is replaced:

| Command | Bench behaviour |
| --- | --- |
| `start` | Registers a runtime and emits `connected` with a synthetic cursor. Reports `supportsSteer: false`. |
| `prompt` | Emits `promptSubmitted` (as the real daemon does), then replays a timeline. |
| `cancel` | Stops the replay and emits `turnFinished { success: false }`. |
| `steer` | Emits `steerRejected`. |
| `closeSession` / `removeSession` | Stops the replay and drops the runtime. |
| `probeProvider` | Codex and Claude report installed, with the stock model catalog. Every other provider reports not installed. |
| `fetchPlanUsage` | Returns no usage. |
| `listProviderSessions` | Returns no sessions. |
| `loadSkills` | Returns an empty catalog. |
| `probeComputerPermissions` | Returns the defaults. |
| `loadUsageHistory`, `rollback`, `fork`, `loadProviderSession`, `forkProviderSession` | Return an error. |

Nothing reaches the network, and no provider process is spawned.

### Running it

```sh
cargo run -p waku-bench-daemon --release -- --timeline burst --rate 1 --exit-after 1
# stdout:
#   WAKU_DAEMON_ADDRESS=127.0.0.1:53412
#   WAKU_DAEMON_TOKEN=…
# then, in another shell:
WAKU_DAEMON_ADDRESS=127.0.0.1:53412 WAKU_DAEMON_TOKEN=… <path to waku>
```

| Flag | Meaning |
| --- | --- |
| `--timeline NAME\|PATH` | The default timeline: a built-in name, or a `.jsonl` / `.jsonl.gz` file. A file joins the fixture as its own session. |
| `--rate FACTOR\|max` | Playback speed. `2` plays twice as fast. `max` ignores timestamps; it still pauses 5 ms after every 1,024 events so the daemon's socket writer, which drains between 25 ms polls, never overflows a client queue. |
| `--loops N` | Repeats the timeline body N times inside one turn. `turnStarted` plays once at the start and `turnFinished` once at the end, and repeated activities get fresh ids. |
| `--listen ADDR` | Default `127.0.0.1:0`. |
| `--provider codex\|claude` | The provider of the fixture sessions. |
| `--history-turns N` | Adds a settled session with N turns of history, in the seed script's shapes. |
| `--exit-after N` | Stops after N replays. |
| `--state-dir DIR` | The throwaway database. It is wiped on every start, and the daemon refuses to wipe a directory it did not create. |
| `--project DIR` | The project directory listed in the fixture. |

`$WAKU_DAEMON_TOKEN` supplies the token if it is set; otherwise one is generated.

The daemon prints a summary on exit, with one row per replay: events, payload bytes, scheduled time, elapsed time, events/s, the worst lag behind schedule, and whether it was canceled. It exits on any of:
- Ctrl-C or SIGTERM;
- a client shutdown, such as quitting the desktop;
- `--exit-after`.

### Fixture task state

The fixture has one project (id `be4c0000-0000-4000-8000-000000000001`) and one session per timeline, titled `Bench · <timeline>`. Every id and timestamp is derived from fixed seeds.

- Each timeline session opens with one short settled turn, because the store only keeps sessions that have started. Any prompt in that session replays its timeline.
- A prompt whose whole text is a timeline name (`burst`, `images`, …) replays that timeline in any session.
- New sessions replay `--timeline`.

### Timelines

The timelines are checked in under `timelines/` as gzip-compressed JSON Lines:

```text
{"timeline":"prose","format":1,"description":"…","events":20003,"durationUs":200070000}
{"atUs":0,"kind":"turnStarted","payload":null}
{"atUs":50000,"kind":"textDelta","payload":"### "}
```

`kind` and `payload` are the daemon's `WireDriverEvent` verbatim. Loading validates every line with the desktop's own `event_from_wire`.

| Name | Content |
| --- | --- |
| `prose` | 20,000 tokens of Markdown (headings, paragraphs, lists, tables, quotes, fences) at 100 tok/s, one token per event. Runs 200 s. |
| `burst` | The same kind of content at 2,000 tok/s. Runs 10 s. |
| `reasoning-fast` | 12,000 reasoning tokens at 1,500 tok/s, then a 2,000-token answer at 500 tok/s. |
| `code-wall` | One 3,000-line fenced Rust block at 1,000 tok/s. |
| `tool-storm` | 300 tool calls (commands, edits, searches, reads), each with a ≥ 50 KB output or diff, one every 100 ms. Each call is sent as an in-progress row and then a completed row. |
| `images` | 20 tool results, each carrying a 320×200 PNG data URL, with captions in between. |

Content comes from the banks in `scripts/seed-mock-sessions.ts`, ported to `src/content.rs` with the same mulberry32 generator.

The fixture sizes are a trade-off between keeping fixtures in the repository and keeping the repository small. Uncompressed JSON totals about 21 MB, of which `tool-storm` is 17 MB (300 × 50 KB is the point of that timeline). Gzipped at level 9, all six total about 1.9 MB, with `tool-storm` at 1.4 MB. That is small enough to commit, which the issue requires: the files are fixtures, not generated at random. It is also small enough to embed in the binary, so the daemon works from any directory.

To regenerate or verify the fixtures:

```sh
cargo run -p waku-bench-daemon -- generate          # rewrite timelines/
cargo run -p waku-bench-daemon -- generate --check  # fail if the files are stale
cargo run -p waku-bench-daemon -- list              # describe the built-ins
```

Gzip output is deterministic: it has no file name, `mtime` is 0, and the compressor is pinned through `Cargo.lock`. The test `checked_in_timelines_match_the_generator` compares the decoded files with the generators.

## Fake provider CLIs (#8)

`fake-codex` and `fake-claude` stream the same timelines through each provider's own protocol. That lets the **real** Waku daemon (`waku-daemon` with its real Codex and Claude drivers) and T3 Code be driven with identical input and no network.

```sh
# Build both fakes and create a private PATH directory with `codex` and
# `claude` symlinks plus a waku-bench.json config:
dir="$(scripts/bench/fake-path.sh --timeline burst --rate 1)"
PATH="$dir:$PATH" <app or daemon>
```

Each fake reads its configuration on every launch. The highest-precedence source wins:

1. The environment variables `WAKU_BENCH_TIMELINE` (a built-in name or a file path), `WAKU_BENCH_RATE`, `WAKU_BENCH_LOOPS` and `WAKU_BENCH_LOG` (a JSONL protocol log).
2. `waku-bench.json` in the directory the executable was invoked from: the symlink's directory, or the first PATH entry holding it.
3. The defaults: `prose` at its recorded rate, played once.

A prompt whose whole text is a built-in timeline name replays that timeline.

For Waku, `command_env` rebuilds a provider's PATH from the login shell. Pointing the provider's **binary override** at `$dir/codex` / `$dir/claude` is therefore more reliable than relying on PATH.

### What is real and what is faked

| | `fake-codex` (`codex app-server`) | `fake-claude` (stream-json) |
| --- | --- | --- |
| Transport | Newline-delimited JSON-RPC on stdio. Only JSON ever reaches stdout. | NDJSON on stdio. Only JSON ever reaches stdout. |
| Handshake | `initialize` returns `userAgent` (`codex_cli_rs/0.160.0`), `codexHome` and the platform; a second `initialize` returns `-32600`. | The `initialize` control request returns an API-key account with no `subscriptionType`, so T3 makes no usage-API call, plus the stock model list. |
| Probes | `account/read` (API key), `model/list` (Waku's fallback catalog), `skills/list` (empty), `account/rateLimits/read` (empty). `--version` prints `codex-cli 0.160.0`, and `app-server --help` exits 0. | `--version` prints `2.1.300 (Claude Code)`. |
| Session | `thread/start` and `thread/resume` return the full thread object (id, timestamps, `source`, `status`, `turns`, …). `thread/name/set` echoes `thread/name/updated`. | `--session-id` / `--resume` set the session id. `system/init` opens each turn. |
| Turn | The `turn/start` response, then `turn/started`, items, `thread/tokenUsage/updated`, and finally `turn/completed` after a 20 ms pause. | The `--replay-user-messages` echo, then `system/init`. Next come partial `stream_event`s (with `--include-partial-messages`), one `assistant` frame per content block, and `tool_use` / `tool_result` pairs. The turn closes with `result` (`modelUsage.contextWindow`). |
| Text and reasoning | One `agentMessage` item with `item/agentMessage/delta`. One `reasoning` item with `item/reasoning/summaryTextDelta` (`summaryIndex` 0). | `text_delta` and `thinking_delta` in `text` and `thinking` blocks. |
| Commands | A `commandExecution` with `aggregatedOutput` and `exitCode` on completion. | `Bash` `tool_use` and a text `tool_result`. |
| Reads, searches, lists | A `commandExecution` with one parsed `commandAction` (`read` / `search` / `listFiles`), as Codex reports them. | `Read`, `Grep` and `Glob`. |
| Edits | A `fileChange` with `changes[{path, diff, kind: update}]`. | `Edit` with `old_string`/`new_string` rebuilt from the diff, plus a `tool_use_result.structuredPatch` carrying the hunks. |
| Images | An `mcpToolCall` (server `bench`) whose `result.content` holds `{type: image, data, mimeType}`. | A `Read` whose `tool_result` content is base64 `image` blocks. |
| Interrupt | `turn/interrupt` leads to `turn/completed` with status `interrupted`. | The `interrupt` control request leads to `result` with `error_during_execution` and `errors`. |
| Structured side requests | A `turn/start` with `outputSchema` (Waku's title generation) and `codex exec --output-schema … --output-last-message FILE -` (T3's text generation) return a value shaped by the schema. `mcp list --json` prints `[]`. | `-p --json-schema …` returns `structured_output` in a `result`. |
| Not supported | Steering (an error, which Waku shows as a rejected steer), goals, background terminals, rollback, fork, compaction, approvals (no server requests are sent) and `item/commandExecution/outputDelta` streaming. | Steering (the message only starts the next turn), permission prompts (`can_use_tool` is never sent), subagents and background tasks, rewind and fork. No session JSONL is written under `~/.claude`, so Waku's native title and rewind-cursor lookups and T3's fork and history readers find nothing. |

The translations are approximations. Ids, timestamps and token counts are synthetic, and the per-block framing follows current Claude Code but is not byte-identical to it. Waku's Codex driver truncates tool output at 16,000 characters, so on that path `tool-storm` rows show truncated output, exactly as with real Codex.

### Tests

- `tests/fake_codex.rs` and `tests/fake_claude.rs` spawn the binaries and drive them through their protocols. They assert the framing, the event order, the fields T3's schemas require, interrupts, title generation and the CLI probes.
- `tests/real_drivers.rs` starts the real `WakuBackend` behind `waku_core::serve`. It points Waku's own Codex and Claude drivers at the fakes, through the private-directory symlink and config mechanism, and asserts the resulting `DriverEvent`s:
  - the text, reasoning, command output, edit diff, image URL, usage and completion;
  - for `burst`, `reasoning-fast` and `code-wall`, text and reasoning byte-identical to the timeline.

### Still to verify end to end (#9)

These tests never launch a GUI. The bench runner still has to show the following:

- the desktop renders each timeline identically across runs against `waku-bench-daemon`;
- T3 Code drives both fakes through its real adapters (Effect schemas, Agent SDK spawn path and probes);
- Waku and T3 show the same visible transcript;
- no network traffic occurs during a run. Disable T3's provider update checks and isolate `HOME` / `CODEX_HOME` / `CLAUDE_CONFIG_DIR`.

# waku-bench-daemon

Deterministic benchmark fixtures for Waku (`docs/fork/audit/waku-perf.md` §4.2).
The crate is in the workspace but not in `default-members`, so ordinary
`cargo build` / `cargo test` skip it; name it with `-p waku-bench-daemon`.

## The bench daemon (#7)

`waku-bench-daemon` is `waku-daemon` as its `main` composes it, over a
throwaway data directory. The desktop cannot tell the two apart, because the
measured path is the production one:

- the server is `waku_core::serve_listeners` with the real `WakuBackend` and
  `DaemonHost`: the orchestrator (the source of truth), its effect workers, the
  provider session manager and the real Codex and Claude drivers, the lifecycle
  service and blob sweeps, served over protocol v8 only (RFC 0003);
- a prompt is an ordinary `message.dispatch`: the orchestrator starts a run, the
  session manager starts the provider CLI, the driver ingests its stream, and
  the `shell` / `thread` streams carry it to every client.

Only two things differ from a user's daemon:

| | Bench behaviour |
| --- | --- |
| Provider CLIs | The Codex and Claude binary overrides in its settings are `codex` / `claude` links to `fake-codex` / `fake-claude` in `<state>/bin`, beside a `waku-bench.json` naming the `--timeline`, `--rate`, `--loops` and a stats file. Every other provider is disabled, so no real provider CLI can start and nothing reaches the network. |
| Data | The data directory is wiped on every start and seeded with the fixture threads (below) before the first connection. |

### Running it

```sh
cargo build -p waku-bench-daemon --release            # the daemon, the fakes and the clients
target/release/waku-bench-daemon --timeline burst --rate 1 --exit-after 1
# stdout:
#   WAKU_DAEMON_ADDRESS=127.0.0.1:53412
#   WAKU_DAEMON_TOKEN=…
#   WAKU_BENCH_PROJECT_ID=be4c0000-0000-4000-8000-000000000001
#   WAKU_BENCH_THREAD_ID=…
# then, in another shell:
WAKU_DAEMON_ADDRESS=127.0.0.1:53412 WAKU_DAEMON_TOKEN=… <path to waku>
```

| Flag | Meaning |
| --- | --- |
| `--timeline NAME\|PATH` | The default timeline: a built-in name, or a `.jsonl` / `.jsonl.gz` file. A file joins the fixture as its own thread. |
| `--rate FACTOR\|max` | Playback speed of the fakes. `2` plays twice as fast. `max` ignores timestamps; it still pauses 5 ms after every 1,024 events. |
| `--loops N` | Repeats the timeline body N times inside one turn. `turnStarted` plays once at the start and `turnFinished` once at the end, and repeated activities get fresh ids. |
| `--listen ADDR` | TCP listener (WebSocket). Default `127.0.0.1:0` unless `--socket` is given. |
| `--socket PATH` | Local socket listener; the printed address is then `unix:PATH`. |
| `--provider codex\|claude` | The provider of the fixture threads (`modelSelection.instance`, with its default model). |
| `--history-turns N` | Adds a settled thread with N completed turns of history, in the seed script's shapes. |
| `--history-sessions N` | Adds N such threads instead of one (memory scenarios). The first keeps the id and content it always had. |
| `--autoplay[=MS]` | Prompts the `--timeline` thread with its timeline MS ms (default 1500) after a client subscribes to that thread, with no UI input. See below. |
| `--exit-after N` | Stops after N runs have replayed a timeline. |
| `--state-dir DIR` | The throwaway data directory (the orchestrator's `state.sqlite`, blobs, drafts, settings, the fakes' links). It is wiped on every start, and the daemon refuses to wipe a directory it did not create. |
| `--project DIR` | The project directory of the fixture. |
| `--fakes DIR` | Where `fake-codex` and `fake-claude` are. Default: beside the `waku-bench-daemon` executable, where Cargo builds them. |

`$WAKU_DAEMON_TOKEN` supplies the token if it is set; otherwise one is generated. As `waku-daemon` does, the daemon removes it from its environment before any provider CLI starts, and with it any `WAKU_BENCH_*` variable, so the fakes read only the daemon's `waku-bench.json`.

Stdout carries `WAKU_DAEMON_ADDRESS`, `WAKU_DAEMON_TOKEN`, `WAKU_BENCH_PROJECT_ID`, `WAKU_BENCH_THREAD_ID` (the `--timeline` thread) and, with `--history-turns`, `WAKU_BENCH_HISTORY_THREAD_ID` (the first history thread), all printed once the fixture is seeded and the listeners are bound. A runner preselects a thread through the desktop's own UI state: `selected_project` (the project id) and `selected_session` (the thread id) in `~/Library/Application Support/Waku Fork/state.json` (`AppState` in `crates/waku-client/src/persistence.rs`, `app_state_version` 1). The desktop opens that thread at launch (`src/startup.rs`), which subscribes to its `thread` stream.

When a run ends, the daemon logs one line on stderr with the replay its fake played, read from the fakes' stats file:

```text
waku-bench-daemon: burst replay finished · 20003 events in 10.00s (2000/s, max lag 0.4 ms) · run completed after 10183 ms
```

`run … after` is the run's own duration in the orchestrator, from its request to its end. On exit (Ctrl-C or SIGTERM, a desktop-owned `daemon.shutdown` such as quitting the app, or `--exit-after`) it prints a summary with one row per replay: events, payload bytes, scheduled time, the fake's elapsed time, events/s, the worst lag behind schedule, the run's duration and status.

### Autoplay (#9)

`--autoplay` makes a run need no clicks or typing:

1. The desktop launches with the timeline's thread selected (`state.json`, above) and subscribes to its `thread` stream, as it does for the selected thread at every launch.
2. That subscription is the signal that the desktop can render the stream. The stream registers with the daemon's delta bus before it reads its snapshot, and the bench daemon watches the bus for the thread (every 50 ms until it appears, then never again).
3. After the delay, the daemon dispatches `message.dispatch` (`queueAfterActive`, as the composer sends) with the timeline's name as its text, through its own orchestrator, as a second client's send would. The run starts, the fake replays the timeline, and the desktop renders it through its thread stream, projection store and rows. Only the composer itself is not exercised.

The daemon logs `waku-bench-daemon: autoplay prompt sent` when it prompts and the replay line above when the run ends. `scripts/bench/harness.py` times its windows from those two lines.

### `replay-client`

The desktop's launch-and-stream path without a window: it connects through the desktop's client layer (`waku_client::store::ProjectionStore`, its reducers and render views), opens `--thread` as the launch opens the selected thread, and follows it until a new turn settles. Against `--autoplay` that subscription starts the replay; with `--prompt TEXT` it sends the message itself.

```sh
target/release/replay-client --address 127.0.0.1:PORT --token T --thread "$WAKU_BENCH_THREAD_ID"
# {"thread":…,"turns":1,"status":"Some(Completed)","view_updates":15,"assistant_bytes":…,"elapsed_ms":…}
```

`scripts/bench/pgo.sh` uses it as PGO training.

### `rpc-latency`

`rpc-latency` connects over protocol v8 and measures sequential round trips: 1,000 each, after 50 warm-up calls, of `settings.get` (the transport floor), `threads.snapshot` (compact, as the desktop subscribes) of `--thread`, `files.searchEntries` over a 500-file project (the composer's `@` file index, every entry) and `threads.search`. It creates that project (and, without `--thread`, an empty thread) through `orchestration.dispatch`, so point it only at a disposable daemon:

```sh
target/release/rpc-latency --address 127.0.0.1:PORT --token T --thread "$WAKU_BENCH_THREAD_ID" --out rpc.json
```

It prints p50/p95/p99, mean and max per call and writes them as JSON (`waku.rpc-latency/2`, microseconds). `--hold SECONDS` then keeps the connection idle for an outside sampler, `--terminal` keeps a daemon terminal open (subscribed, its shell at the prompt) during the hold, and `--supervise DAEMON` launches a `waku-daemon` through the desktop's `DaemonSupervisor` instead of connecting (give it a private `HOME` and `WAKU_DAEMON_SOCKET`).

### Fixture threads

The fixture has one project (id `be4c0000-0000-4000-8000-000000000001`) and one thread per timeline, titled `Bench · <timeline>`, plus the history threads. Projects and threads are created with `project.create` and `thread.create`; their turns are committed through the orchestrator (`ingest_decision`) as completed runs with the messages and turn items a provider turn stores (user message, command and search tool calls, assistant message), so the streams, history paging and search serve real projection rows. Every id, timestamp and text is derived from fixed seeds.

- Each timeline thread opens with one short settled turn saying what a prompt in it replays.
- A prompt whose whole text is a timeline name (`burst`, `images`, …) replays that timeline in any thread; any other prompt replays `--timeline`. The fakes cannot see which thread a prompt belongs to, so typing in `Bench · burst` replays `burst` only when the text is `burst`.
- A history turn has 1–3 tool calls and a 300–1,400-byte Markdown answer; `--history-turns 1000` makes a 2,000-message thread.

### Timelines

The timelines are checked in under `timelines/` as gzip-compressed JSON Lines:

```text
{"timeline":"prose","format":1,"description":"…","events":20003,"durationUs":200070000}
{"atUs":0,"kind":"turnStarted","payload":null}
{"atUs":50000,"kind":"textDelta","payload":"### "}
```

`kind` and `payload` describe one provider event in a provider-neutral shape, which the fakes translate into each provider's protocol. Loading validates every line (`timeline::validate_event`):

| `kind` | `payload` |
| --- | --- |
| `turnStarted` | `null` |
| `textDelta`, `reasoningDelta`, `error` | a string |
| `richActivity` | an `ActivityItem` (`waku_protocol::model`): tool calls, edits, images |
| `activity` | `{id?, kind, title, detail?, complete}` |
| `usageUpdated` | `{contextTokens?, contextWindow?}` |
| `turnFinished` | `{success, summary?}` |

| Name | Content |
| --- | --- |
| `prose` | 20,000 tokens of Markdown (headings, paragraphs, lists, tables, quotes, fences) at 100 tok/s, one token per event. Runs 200 s. |
| `burst` | The same kind of content at 2,000 tok/s. Runs 10 s. |
| `reasoning-fast` | 12,000 reasoning tokens at 1,500 tok/s, then a 2,000-token answer at 500 tok/s. |
| `code-wall` | One 3,000-line fenced Rust block at 1,000 tok/s. |
| `tool-storm` | 300 tool calls (commands, edits, searches, reads), each with a ≥ 50 KB output or diff, one every 100 ms. Each call is sent as an in-progress row and then a completed row. |
| `images` | 20 tool results, each carrying a 320×200 PNG data URL, with captions in between. |

Content comes from the banks in `src/content.rs` (originally ported from a seeding script for the legacy session database) with a mulberry32 generator.

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

1. The environment variables `WAKU_BENCH_TIMELINE` (a built-in name or a file path), `WAKU_BENCH_RATE`, `WAKU_BENCH_LOOPS`, `WAKU_BENCH_LOG` (a JSONL protocol log) and `WAKU_BENCH_STATS` (one JSON line of replay statistics per replay, written before the turn completes; the bench daemon's run report reads it).
2. `waku-bench.json` in the directory the executable was invoked from: the symlink's directory, or the first PATH entry holding it. Its keys are `timeline`, `rate`, `loops`, `log` and `stats`.
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
- `tests/bench_daemon.rs` starts the bench daemon and checks the fixture over v8 (projects, the shell's thread rows, a history thread's bounded snapshot, search, the providers offered), a prompt replayed by the fake Claude CLI, an interrupt that cancels the replay, and `--exit-after`.
- `tests/autoplay.rs` checks that `--autoplay` waits for a `thread` subscription, honors its delay, prompts once, and delivers the whole run to the subscriber.
- `tests/orchestrated_daemon.rs`, `tests/desktop_client.rs`, `tests/desktop_surfaces.rs` and `tests/restart_recovery.rs` compose the daemon themselves and drive Waku's real Codex and Claude drivers through the fakes: transcripts byte-identical to the timeline, clients that come and go, the desktop's projection store and surfaces, restarts.

### Still to verify end to end (#9)

These tests never launch a GUI. `scripts/bench/baseline.sh` drives the real desktop through every timeline against `waku-bench-daemon` (see `docs/fork/bench/baseline.md`). Still open:

- that the desktop renders each timeline identically across runs (the runs agree on counters, but nothing compares frames);
- T3 Code drives both fakes through its real adapters (Effect schemas, Agent SDK spawn path and probes);
- Waku and T3 show the same visible transcript;
- no network traffic occurs during a run. Disable T3's provider update checks and isolate `HOME` / `CODEX_HOME` / `CLAUDE_CONFIG_DIR`.

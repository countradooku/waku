# Baseline performance: Waku (#9)

This is the reference that later performance claims are measured against. It covers **Waku only**. The owner chose to measure Waku now and T3 Code later; the T3 column is scripted in [`scripts/bench/t3.md`](../../../scripts/bench/t3.md) but has not been run. Every row that needs T3, Instruments or `powermetrics` is listed under [Not measured yet](#not-measured-yet).

- Raw data: [`raw/baseline-2026-10-20-summary.json`](raw/baseline-2026-10-20-summary.json). It holds the medians, the min–max range and the per-run values of every metric below, plus the machine record.
- Reproduce: `BENCH_ROUNDS=3 scripts/bench/baseline.sh` (see [Method](#method)).

## Machine and build

| | |
| --- | --- |
| Date | 2026-10-20 |
| Machine | Mac17,8, Apple M5 Pro (18 cores), 48 GB, on AC power |
| macOS | 27.0.1 (26A434) |
| Display | Built-in Liquid Retina XDR, 3456×2234; `NSScreen.maximumFramesPerSecond` = **120** (ProMotion) |
| App commit | `37942d1` on `phase/0-baseline`: the commit the release binaries were built from (`cargo build --release --locked --features perf-counters`). `machine.json` says the tree was dirty, but the only uncommitted file was `scripts/bench/harness.py`. |
| Toolchain | rustc 1.99.0 (Homebrew) |
| Load | This was the owner's working machine, with Chrome, Claude, ChatGPT/Codex, Slack and Zed open. The 1-minute load average after each run was 2.5–7.4. Treat small differences as noise. |

## Results against the targets

PLAN §5.1 targets. "App" is the Waku process; "daemon" is the daemon process. In these runs the daemon is `waku-bench-daemon`, so no provider CLI runs. Values are medians, with min–max in parentheses.

| Metric | Target | Waku baseline | Verdict |
| --- | --- | --- | --- |
| Idle frames | < 1 frame/s, composer unfocused | **0** frames/s (0–1.96) | met |
| Idle wakeups | < 5/s total | app **60**/s (43–168) + daemon **92**/s = **~150/s** | not met (30×) |
| Idle CPU | < 0.1% | app **0.31%** (0.20–1.45) + daemon **0.18%** | not met |
| Streaming CPU | ≤ ⅓ of T3's shell CPU at a higher visual update rate | app + daemon: prose **6.9%**, burst **20.0%**, reasoning-fast **14.3%**, code-wall **15.1%**, tool-storm **32.8%**. Visual update rate: 7–8 commits/s and 28–44 frames/s while streaming. | T3 not measured |
| Frame time | p99 < 8.3 ms at 120 Hz; 0 hitches during saves | Not measured. Saves cost **~10 ms of UI-thread time each** (prose: 191 saves, 1.9 s), longer than one 120 Hz frame. | not measured; saves likely miss it |
| Memory | Flat once N ≥ 5 long sessions; < ⅓ of T3 | App RSS after visiting N 2,000-message sessions: **134 / 158 / 225 MB** for N = 1 / 5 / 20, about +4.5 MB per session past 5. Daemon: 118 / 153 / 210 MB footprint. | not flat |
| Cold start | First content < 600 ms | **724 ms** (one sample, no `purge`) | not met |
| Warm start | First content < 250 ms | **305 ms** (296–369) | not met |
| 10k-line diff | First paint < 150 ms; scroll p99 < 8.3 ms | Not measured | — |
| Terminal | ≥ Zed's throughput; echo < 1 frame + PTY | Not measured | — |
| Local RPC | p50 < 1 ms | **52.2 ms** p50, 52.5 ms p99, for every request kind | not met (50×) |

## Detailed results

### Streaming (one full replay each, timeline at its recorded rate)

`app` and `daemon` are each a process tree measured with `proc_pid_rusage` over the replay window, from the autoplay prompt to the daemon's "replay finished". `counters.*` come from the app's perf-counters report over the same window.

| metric | burst | code-wall | prose | reasoning-fast | tool-storm |
| --- | ---: | ---: | ---: | ---: | ---: |
| Replay (s) | 10.07 | 25.72 | 200.07 | 12.17 | 30.84 |
| App CPU % | 18.53 (13.63–20.03) | 13.48 (9.99–16.96) | 6.29 (3.87–8.71) | 12.35 (9.98–14.72) | 24.39 (23.87–24.91) |
| Daemon CPU % | 1.44 (1.35–1.84) | 1.61 | 0.59 | 1.94 (1.76–2.11) | 8.43 (8.17–8.70) |
| App CPU (s) | 1.86 | 3.46 | 12.58 | 1.51 | 7.53 |
| Daemon CPU (s) | 0.14 | 0.41 | 1.20 | 0.24 | 2.60 |
| App interrupt wakeups/s | 217 (174–221) | 180 (144–216) | 91 (65–117) | 197 (174–220) | 224 |
| Daemon interrupt wakeups/s | 2,077 | 1,062 | 191 | 1,239 | 123 |
| Frames/s | 31.4 (21.6–35.0) | 28.2 (20.8–35.6) | 11.1 (6.3–15.8) | 30.1 (23.1–37.1) | 43.7 |
| Stream commits/s (mean / peak) | 7.36 / 9 | 7.96 / 9 | 6.70 / 8.5 | 7.07 / 9 | 0.27 / 7 |
| Transcript renders per commit | 4.25 | 3.44 | 1.59 | 3.53 | 123 (few commits) |
| Rows built per commit frame | 2.19 | 2.15 | 2.64 | 4.88 | 7.65 |
| UI-thread KB allocated per commit | 23.9 | 16.0 | 12.5 | 17.9 | 3,911 |
| Sidebar renders during the replay | 340 | 675 | 2,093 | 346 | 1,332 |
| Right-panel renders | 0 | 0 | 0 | 0 | 0 |
| UI time in `Waku::save` (ms) | 103 | 246 | 1,909 | 136 | 299 |
| App RSS peak (MB) | 157 | 168 | 138 | 124 | 483 |
| App footprint peak / final (MB) | 329 / 237 | 330 / 267 | 314 / 195 | 297 / 297 | 668 / 619 |
| Daemon footprint final (MB) | 79 | 52 | 52 | 78 | 503 |

Valid runs: burst 3, every other timeline 2. See [Window visibility](#window-visibility-and-discarded-runs).

### Idle (app open 60 s on a short settled session, measured over the last 50 s)

| metric | value |
| --- | ---: |
| App CPU % | 0.31 (0.20–1.45) |
| App interrupt wakeups/s | 59.8 (42.5–167.7) |
| App package-idle wakeups/s | 0.8 |
| Daemon CPU % | 0.18 (0.17–0.22) |
| Daemon interrupt wakeups/s | 91.9 (88.8–92.2) |
| Frames/s | 0 (0–1.96) |
| App footprint / RSS (MB) | 137 / 108 |
| Daemon footprint (MB) | 64 |

The outlier run (1.45% CPU, 1.96 frames/s, 168 wakeups/s) most likely had the pointer or focus on the window. The harness does not control input on a shared machine.

### Memory: N sessions of 1,000 turns (2,000 messages) each, every session visited

The bench daemon seeds `--history-sessions N --history-turns 1000`. The app's session visitor (`WAKU_PERF_VISIT_SESSIONS_MS=2500`) selects every started session once through the sidebar-click path: the 6 timeline sessions plus the N history sessions. The harness waits until the counters report all visits, then lets the app idle for 20 s.

| metric | N = 1 | N = 5 | N = 20 |
| --- | ---: | ---: | ---: |
| App footprint right after launch (MB) | 306 | 309 | 307 |
| App footprint after visits + 20 s (MB) | 200 (162–308) | 240 (227–334) | 274 (270–286) |
| App footprint peak (MB) | 308 | 334 | 327 (300–413) |
| App RSS after visits + 20 s (MB) | 134 | 158 | 225 |
| Daemon footprint (MB) | 118 | 153 | 210 |
| Sessions visited | 7 | 11 | 26 |

RSS is the stable signal here; its spread is under 13 MB. Phys footprint swings by about 100 MB between runs of the same scenario because it includes the window's GPU-backed surfaces, which depend on whether the window was visible. RSS grows by about 6 MB per session from 1 to 5 and 4.5 MB per session from 5 to 20, so per-session state is not bounded yet. The daemon grows too, by about 3.8 MB per session, because hydration caches every visited session.

### Start (200 sessions in the database, a 10-turn session selected)

Times run from the kernel's process start time (`proc_pidinfo`) to perf-counters milestones (`startup.*`, see `docs/performance.md`).

| metric | cold (first launch after build) | warm |
| --- | ---: | ---: |
| Process start → `waku::run` (`init_ms`) | 371 | 5.9 (5.8–11.1) |
| → first frame | 713 | 293 (287–354) |
| → first transcript row (first content) | 724 | 305 (296–369) |
| RSS / footprint at first content + 1 s (MB) | 117 / 291 | 116 / 290 |

"Cold" here is one sample: the first launch of the freshly linked binary, with the page cache not purged (`purge` needs sudo). Almost all of the cold penalty, 365 of 419 ms, falls before `main`. That points at macOS's first-launch assessment of a new binary rather than at app code. A truly cold-cache launch of a signed, installed build is still unmeasured. Warm, about 290 ms passes between `main` and the first frame.

### Local RPC latency (`rpc-latency` against the release `waku-daemon`, disposable HOME)

1,000 sequential requests of each kind after 50 warm-up requests, one in flight at a time, over a loopback WebSocket.

| request | p50 (ms) | p95 (ms) | p99 (ms) |
| --- | ---: | ---: | ---: |
| `getSettings` (transport floor) | 52.18 | 52.47 | 52.53 |
| `hydrateSession` (2 messages) | 52.21 | 52.51 | 52.60 |
| `workspace.listProjectFiles` (500 files) | 52.21 | 52.34 | 52.39 |
| `searchSessionMessages` | 52.16 | 52.45 | 52.53 |

The work itself is negligible; every request kind costs the same 52 ms. Two 25 ms polling loops account for it:

1. The client's socket thread drains its outgoing queue only at the top of its loop, after a read that times out every 25 ms (`READ_POLL_INTERVAL`, `crates/waku-client/src/client.rs`).
2. The server answers on the same pattern: the response sits in the connection's outgoing channel until the next 25 ms `SOCKET_POLL_INTERVAL` read timeout (`crates/waku-core/src/server.rs`).

The audit estimated about 25 ms. The real figure is twice that, and the 1 ms target needs evented I/O on both ends (audit §3.3). Polling also explains most of the idle daemon's ~92 wakeups/s: the listener sleeps 25 ms between `accept` attempts (`ACCEPT_POLL_INTERVAL`) and the desktop's connection reads with a 25 ms timeout, which makes about 80/s before any work.

## What the numbers say (for Phase 1)

- **RPC latency and idle wakeups** both come from the 25 ms polling sockets (audit §3.3). They are the largest misses against target: 50× on RPC, 30× on wakeups.
- **Saves run on the UI thread**, at about 10 ms each, and a streaming turn saves about once a second (191 saves during the 200 s prose replay). Each save is longer than one frame at 120 Hz (audit §3.1).
- **The commit cadence holds**: 6.7–8.0 commits/s mean, at most 9 in any one second. But frames run at 28–44/s while streaming. Transcript renders per commit are 1.6–4.3 (budget ≤ 1) and the sidebar re-renders throughout (budget 0 after audit §3.4). That fits pulse-driven frames rebuilding the root.
- **The daemon writes per event**: its interrupt wakeups track the event rate (2,077/s for `burst` at 2,000 events/s).
- **`tool-storm` is the heaviest scenario**: 24% app CPU, 3.9 MB of UI-thread allocation per commit, and 619 MB app plus 503 MB daemon footprint after 300 × 50 KB tool outputs. Those outputs are retained in full on both sides (audit §3.7, §3.8).
- **Memory is not bounded per session** (+4.5 MB RSS per visited 2,000-message session in the app, +3.8 MB in the daemon).

## Method

`scripts/bench/baseline.sh` builds the release binaries and runs `scripts/bench/harness.py`. The scenarios interleave round by round (start, idle, the five timelines, memory 1/5/20, rpc, then the next round), so that drift spreads over all of them. The cold start runs once, before round 1.

**Isolation.** Every app and daemon process runs with `HOME` set inside the run's directory (`temp/bench/…`, gitignored). The app's `~/.waku-fork/app.json`, its `~/Library/Application Support/Waku Fork/state.json` and the real daemon's database all land there. The app connects to an external `waku-bench-daemon` through `WAKU_DAEMON_ADDRESS` / `WAKU_DAEMON_TOKEN`, so it never spawns its own daemon. `WAKU_DISABLE_ANALYTICS=1` is set, and analytics is off by default anyway. Bench project directories live in `$TMPDIR`, outside the checkout, so `git` and the file index do not see the Waku repository. After each run the harness checks the isolated HOME for `.codex` / `.claude`, the sign that a real provider CLI ran; every run is clean. (The first smoke run was not: the bench daemon forwarded slash-command discovery to the real backend, which started the real `codex app-server`. The bench daemon now answers it without running a CLI.) The owner's real Waku data was never read or written.

**No UI input.** The runs need no clicks or typing:

- `waku-bench-daemon --autoplay` starts the selected session's runtime before the app launches. It prompts the timeline once the desktop attaches, so the desktop renders the stream through its ordinary attach and event-pump path, as for a second client's prompt (crate README, "Autoplay").
- The harness preselects that session by writing `selected_project` / `selected_session` into the isolated `state.json`.
- The memory scenarios visit sessions with the `perf-counters`-only `WAKU_PERF_VISIT_SESSIONS_MS` hook.

**Per run, the harness collects:**

- the perf-counters JSON (a compact copy without the series goes into the result);
- CPU time for the app and daemon process trees, from `proc_pid_rusage` (`RUSAGE_INFO_V4`): user + sys, plus reaped children, converted from Mach ticks;
- interrupt and package-idle wakeups, from the same per-task counters `powermetrics`' tasks sampler reports (not cross-checked against `powermetrics`, which needs sudo);
- RSS and phys footprint, sampled every 500 ms for the peak and read at the end for the final value, cross-checked with `footprint -p` (the two agree within 1 MB);
- wall time;
- the daemon's replay statistics.

Results are medians of the valid runs, at N = 3 rounds instead of the issue's 5 to save time. `BENCH_ROUNDS=5` runs the full count.

### Window visibility and discarded runs

A GPUI window that is hidden or fully covered draws no frames, while the stream keeps committing. A run like that measures a background app. On this shared machine the owner's full-screen windows often covered the app window after launch. The harness therefore keeps a streaming run only if at least 80% of its stream commits reached a drawn frame (`counters.presented_commit_ratio`).

- Discarded: burst 2 runs, prose 3, reasoning-fast 3, code-wall 3, tool-storm 3. Their files are under `temp/bench/baseline-20261020/discarded/` on the measuring machine, not committed. Most show about 1% app CPU at near-zero frames; a few were drawn for part of the replay.
- Two extra rounds (4 and 5) were run to replace them. Only one of those runs (burst) had its window drawn, so most timelines have 2 valid runs, not 3.
- Idle and memory runs have no such check (idle draws nothing either way). Their footprint spread reflects it; their RSS does not.
- The harness also records `window_visible`, an estimate from window bounds, but it cannot see Spaces, so it is informational only.

For clean numbers, run the matrix on a machine nobody is using, with the app window left in front.

## Not measured yet

- **T3 Code column**: every comparison row. Runbook: [`scripts/bench/t3.md`](../../../scripts/bench/t3.md).
- **Frame time p99 and hitches** (scroll through 2,000 messages with terminal and diff open, streaming, panel toggle). These need the Instruments Animation Hitches template, or the fork hook from audit §4.3 that timestamps `Window::draw` and present. Perf counters count frames but do not time them.
- **`powermetrics` wakeups and energy**: needs sudo. The wakeup rows above use `proc_pid_rusage` instead.
- **True cold start** (`purge` needs sudo) and **time to interactive** (composer accepts a key). There is also no Instruments App Launch trace.
- **10k-line diff open and scroll**, and **terminal throughput and echo latency**: no scripted fixture repository or `vtebench` run yet.
- **Provider CLIs in the loop for Waku**: these runs use `waku-bench-daemon`, which has no provider process. `t3.md` describes the like-for-like variant (real `waku-daemon` + `fake-codex` / `fake-claude`) for the T3 comparison.
- **Idle with 3 sessions, the terminal panel open and 5 minutes** (audit §4.3): this baseline idles on one session for 60 s with the right panel in its default state.
- **Visual check that each timeline renders identically across runs**: the counters agree between runs, but nothing compares frames.

# Phase 2 results (Waku only)

- **Run:** `BENCH_ROUNDS=3 scripts/bench/baseline.sh` on the same machine as [Phase 0](baseline.md) and [Phase 1](phase1.md): Apple M5 Pro, 48 GB, macOS 27.0.1, 120 Hz.
- **Build:** commit `f2b91bc`, the whole phase: orchestrator, DriverV2, protocol v8 only (v7 deleted, D6), daemon-owned transcripts.
- **Raw data:** [`raw/phase2-2026-10-21-summary.json`](raw/phase2-2026-10-21-summary.json). Prose was rerun on its own because the full pass discarded all three of its runs (window covered).
- **Reading the table:** medians, with Phase 1 values from `phase1.md` and its raw summary.

## The bench is now the real daemon

Phase 1 measured a bench daemon that replayed timelines in-process and stored nothing. Phase 2's bench daemon is the `waku-daemon` composition: every event goes through the orchestrator (decide → one SQLite transaction → projections → streams), the fake CLIs run as their own processes (`providers` columns), and clients speak v8. Daemon CPU while streaming is therefore not comparable one to one: it now includes persistence, and the replay moved out to the fake CLI.

## Before and after

| Metric | Target | Phase 1 | Phase 2 | Verdict |
|---|---|---|---|---|
| Local RPC p50 (settings / thread snapshot) | < 1 ms | 0.059 / 0.060 ms | 0.050 / 0.061 ms | met |
| Thread search p50 | — | 0.179 ms | 0.106 ms | improved (FTS5, #65) |
| File search p50 (500 files) | — | 9.9 ms | 9.6 ms | flat |
| Warm start, first content | < 250 ms | 97 ms | 104 ms | met |
| Cold start (first launch after build), first content | < 600 ms | 895 ms | 1,086 ms (one run; an earlier Phase 2 pass: 619 ms) | not met; dominated by macOS checking a new binary |
| Idle frames/s | < 1 | 0 | 0.02 | met |
| Idle app CPU, window visible | < 0.1% | 0.13% (range 0.09–0.93) | 0.90% | not met (#183) |
| Idle app wakeups, window visible | — | 4.1/s (range 4.1–127.5) | 123/s | the display link (#183) |
| Idle daemon wakeups (GUI client attached) | — | 17.9/s | 7.0/s | improved |
| App RSS with 1 / 5 / 20 long threads | flat after 5 | 135 / 158 / 171 MB | 117 / 122 / 129 MB | met |
| Daemon footprint with 1 / 5 / 20 long threads | — | 105 / 173 / 340 MB | 43 / 59 / 77 MB | improved (bounded snapshots, #54/#55) |
| Streaming CPU, burst (app + daemon + fake CLI) | ≤ ⅓ of T3 | 20.9% + 2.1% | 21.1% + 7.0% + 1.9% | app flat; daemon now persists every event; T3 not measured (#177) |
| Daemon wakeups while streaming (burst / reasoning-fast / code-wall) | — | 2,062 / 1,238 / 1,056 /s | 74 / 47 / 40 /s | improved ~25× |
| tool-storm: daemon footprint / app RSS peak | — | 779 / 456 MB | 66 / 138 MB | improved (wire projection, #57) |
| Transcript renders per stream commit | ≤ 1 | 4.2–4.6 | 4.2–4.6 | not met (fluidity track) |
| Sidebar renders during a 10 s burst | 0 unless visible change | 293 | 296 | not met (fluidity track) |
| Right-panel renders while streaming | 0 | 0 | 0 | met |
| UI time in save | ~0 | 0.17–0.45 ms | 0 | met (transcripts are daemon-owned, #55) |
| UI allocation per commit (burst) | — | 12.3 KB | 6.2 KB | improved |

Per timeline (app / daemon / fake CLI CPU, frames/s):

| Timeline | Phase 1 app / daemon | Phase 2 app / daemon / CLI | Phase 2 frames/s |
|---|---|---|---|
| prose | — (no valid run) | 23.0% / 1.4% / 0.3% | 33.6 |
| burst | 20.9% / 2.1% | 21.1% / 7.0% / 1.9% | 35.6 |
| reasoning-fast | 11.3% / 2.0% | 13.9% / 5.6% / 1.7% | 38.1 |
| code-wall | 12.5% / 1.6% | 16.3% / 4.3% / 1.5% | 34.2 |
| tool-storm | 26.7% / 8.5% | 27.1% / 3.7% / 0.2% | 48.7 |

## Exit criteria

"Kill the desktop mid-turn: the turn completes, persists and re-renders on reconnect; two clients stay in sync." Checked with the bench daemon and the real desktop:

- A `burst` run at quarter speed (40 s) started from the desktop; a second client (`replay-client`) followed the same thread.
- The desktop was killed with `SIGKILL` 8 s into the turn. The daemon kept running and the run completed: 20,003 events in 40.3 s.
- The second client saw the whole turn through to `Completed` (2,400 view updates).
- The relaunched desktop showed the completed turn and listed the thread under Today.

Restart recovery of the daemon itself (`RecoverThread` / `ContinueRun`) is covered by the replay suite's `daemon_restart` scenarios.

## What did not move, and why

- **Idle wakeups and CPU with the window visible.** GPUI's `CVDisplayLink` keeps firing every vsync even when nothing is invalidated, and each tick calls into the app. Phase 1's 4.1/s median came from runs whose window was not visible, where the link stops; its own range already reached 127.5/s. Pausing the link while idle is a GPUI change: #183.
- **Streaming render counts.** Unchanged from Phase 1: the transcript's own animations drive frames between commits, and the sidebar renders at frame rate while a thread is working. The fluidity track (#32, #33, #36).
- **Daemon CPU while streaming.** Higher than Phase 1 because every event is now committed to SQLite and projected; the fake CLI's replay is counted separately. Wakeups fell about 25×. Coalescing ingest further is Phase 3 material.
- **App CPU on code-wall and reasoning-fast** is 2–4 points higher than Phase 1's single valid runs. Allocation per commit halved, so the cost is in rendering, the same fluidity work.

## Measurement notes

- The harness now raises the app window when a run starts: a covered GPUI window draws nothing, and its run is discarded by the presented-commit ratio.
- Seeded bench threads now carry `last_reply_at` like dispatched ones, so the sidebar lists all nine threads during every scenario (it showed only the running thread before that fix).
- The memory scenarios visit every daemon thread through the perf hook, which now waits for the shell stream.
- Web thread switching was not benchmarked. In a dev build (`vite dev`) inside a hidden browser pane, animation frames ran at 1.5/s; with the pane visible a switch took 0.07–1.7 s while the daemon answered the subscription in about 50 ms. Measure on a production build.

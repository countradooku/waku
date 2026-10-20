# Phase 1 results (Waku only)

- **Run:** `BENCH_ROUNDS=3 scripts/bench/baseline.sh` on the same machine as the [Phase 0 baseline](baseline.md): Apple M5 Pro, 48 GB, macOS 27.0.1, 120 Hz.
- **Build:** commit `515159a`. That is every Phase 1 change except #178; #178's numbers were measured separately with `rpc-latency --supervise --hold`.
- **Raw data:** [`raw/phase1-2026-10-20-summary.json`](raw/phase1-2026-10-20-summary.json).
- **Reading the table:** medians, with Phase 0 values from `baseline.md`.

## Before and after

| Metric | Target | Phase 0 | Phase 1 | Verdict |
|---|---|---|---|---|
| Local RPC p50 (`getSettings` / `hydrateSession`) | < 1 ms | 52.2 ms | 0.059 / 0.060 ms | met |
| Idle frames/s | < 1 | 0 | 0 | met |
| Idle app CPU | < 0.1% | 0.31% | 0.13% | close |
| Idle app wakeups | — | ~60/s | 4.1/s | improved |
| Idle daemon wakeups (GUI client attached) | — | ~92/s | 17.9/s, then ~0.6/s with #178 | improved |
| Idle total wakeups, headless stand-in with a terminal open (#178) | < 5/s | 172.5/s | 1.3/s | met |
| UI time in save, burst / code-wall / tool-storm | ~0 | 103 / 246 / 299 ms | 0.17 / 0.38 / 0.45 ms | met |
| Warm start, first content | < 250 ms | 305 ms | 97 ms | met |
| Cold start (first launch after build), first content | < 600 ms | 724 ms | 895 ms | not met (755 ms is spent before `main`: macOS checking a new binary) |
| App RSS with 1 / 5 / 20 long sessions | flat after 5 | 134 / 158 / 225 MB | 135 / 158 / 171 MB | mostly flat (+13 MB from 5 to 20, was +67) |
| Daemon footprint with 1 / 5 / 20 sessions | — | 118 / 153 / 210 MB | 105 / 173 / 340 MB | worse at 20; daemon-side caching was out of scope |
| Streaming CPU, burst (app + daemon) | ≤ ⅓ of T3 | 20.0% | 20.9% + 2.1% | flat; T3 not measured (#177) |
| Transcript renders per stream commit | ≤ 1 | 1.6–4.3 | 4.2–4.6 | not met |
| Sidebar renders during a 10 s burst | 0 unless visible change | throughout | 293 (~29/s) | not met |
| Right-panel renders while streaming | 0 | throughout | 0 | met |
| Wire size of the `burst` timeline | — | 4.5 MB | 0.2 MB | improved |
| Terminal full snapshot (bench) | — | 458 µs | 79 µs | improved |

## What did not move, and why

- **Streaming render counts.**
  - The transcript still renders about 4.5 times per commit, because its own 30 Hz veil and 15 Hz working dots drive frames between 8.3 Hz commits.
  - The sidebar renders at frame rate while the selected session's "working" indicator animates.
  - Fixing this means rendering at display rate with cached rows and splitting the root entity: the fluidity track, #32, #33 and #36. Numbers are posted there.
- **Daemon wakeups while streaming.** These are 1,000–2,000/s on fast timelines. The bench daemon replays one event per scheduled timestamp, so this is partly the fixture; the real daemon's 8–16 ms delta coalescing should bound it. Tracked for the Phase 2 orchestrator ingest path.
- **Daemon memory.** It grows with hydrated sessions (340 MB at 20). This is addressed by daemon-owned projections and bounded snapshots in Phase 2 (#54, #55).

## Measurement caveats

- Most streaming runs were discarded because the app window was covered: a covered GPUI window draws nothing. Burst, code-wall, reasoning-fast and tool-storm have one valid run each; prose has none.
- Re-run on an idle machine with the window in front (part of #177).

#!/usr/bin/env bash
# Waku baseline performance matrix (#9).
#
# Builds the release binaries (the app with `--features perf-counters`), runs
# every scenario BENCH_ROUNDS times through scripts/bench/harness.py, and writes
# the medians as JSON. See docs/fork/bench/baseline.md for the method.
#
#   scripts/bench/baseline.sh                      # everything, 5 rounds
#   BENCH_ROUNDS=3 scripts/bench/baseline.sh       # what the committed results used
#   BENCH_SCENARIOS=idle,burst scripts/bench/baseline.sh
#
# Environment:
#   BENCH_ROUNDS      rounds per scenario (default 5; the median is reported)
#   BENCH_SCENARIOS   comma-separated subset of: start, idle, prose, burst,
#                     reasoning-fast, code-wall, tool-storm, memory-1,
#                     memory-5, memory-20, rpc (default: all)
#   BENCH_OUT         working directory (default temp/bench/<timestamp>, which
#                     .gitignore covers; holds per-run logs, counters and the
#                     isolated HOME directories)
#   BENCH_RAW         where to write the summary JSON
#                     (default $BENCH_OUT/summary.json)
#   BENCH_SKIP_BUILD  set to 1 to reuse target/release as it is
#
# The app opens a window for every run. It talks only to the bench daemon (the
# real daemon composition over protocol v8, whose Codex and Claude CLIs are
# fake-codex / fake-claude replaying the timelines), with HOME pointed into
# BENCH_OUT, so your own Waku data is never read or written. RPC latency runs
# rpc-latency against the bench daemon, no app. Keep the machine otherwise idle
# and on AC power while it runs; the full matrix takes about 45 minutes at 3
# rounds.

set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "baseline.sh measures with proc_pid_rusage and footprint: macOS only" >&2
  exit 1
fi

rounds="${BENCH_ROUNDS:-5}"
scenarios="${BENCH_SCENARIOS:-}"
out="${BENCH_OUT:-temp/bench/$(date +%Y%m%d-%H%M%S)}"
raw="${BENCH_RAW:-$out/summary.json}"

if [[ "${BENCH_SKIP_BUILD:-0}" != 1 ]]; then
  echo "building release binaries" >&2
  cargo build --release --locked --features perf-counters --bin waku
  # waku-bench-daemon, fake-codex, fake-claude, replay-client, rpc-latency.
  cargo build --release --locked -p waku-bench-daemon
fi

mkdir -p "$out"
python3 scripts/bench/harness.py run \
  --out "$out" \
  --rounds "$rounds" \
  ${scenarios:+--scenarios "$scenarios"}
python3 scripts/bench/harness.py summarize --out "$out" --raw "$raw" --markdown "$out/summary.md" >/dev/null
echo "results: $out/results/  summary: $raw, $out/summary.md" >&2

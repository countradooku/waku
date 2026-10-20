#!/usr/bin/env bash
# Same-machine A/B benchmark comparison between two Git revisions.
#
#   scripts/bench/compare.sh <base-ref> <head-ref>
#
# Builds every Criterion bench target (`cargo bench --bench '*'`) of both
# revisions, then runs them in alternating rounds (base, head, head, base, ...)
# on this machine and hands the per-round medians to compare.py, which fails
# when a benchmark's median slowed by more than the threshold *and* by more
# than the run-to-run noise it measured. `<head-ref>` may be `.` to compare
# the current working tree, uncommitted edits included.
#
# Environment:
#   BENCH_ROUNDS       alternating rounds per side (default 3)
#   BENCH_THRESHOLD    allowed slowdown as a fraction (default 0.10)
#   BENCH_WARMUP       Criterion warm-up seconds per benchmark (default 1)
#   BENCH_MEASUREMENT  Criterion measurement seconds per benchmark (default 3)
#   BENCH_FILTER       Criterion name filter, e.g. `md/` (default: all)
#   BENCH_OUT          work directory (default: $CARGO_TARGET_DIR/bench-compare)
#
# Exit status: 0 = no regression (or nothing to compare), 1 = regression,
# 2 = usage or build error.

set -euo pipefail

if [[ $# -ne 2 ]]; then
  sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
fi

base_ref=$1
head_ref=$2
repo=$(git rev-parse --show-toplevel)
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$repo/target}
out=${BENCH_OUT:-$CARGO_TARGET_DIR/bench-compare}
rounds=${BENCH_ROUNDS:-3}
threshold=${BENCH_THRESHOLD:-0.10}
warmup=${BENCH_WARMUP:-1}
measurement=${BENCH_MEASUREMENT:-3}
filter=${BENCH_FILTER:-}
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

cleanup() {
  for side in base head; do
    if [[ -d $out/src/$side ]]; then
      git -C "$repo" worktree remove --force "$out/src/$side" >/dev/null 2>&1 || true
    fi
  done
  git -C "$repo" worktree prune >/dev/null 2>&1 || true
  # Copied bench executables are large; keep them and the source copies out
  # of the CI target cache.
  rm -rf "$out/bin" "$out/src"
}
trap cleanup EXIT

cleanup
rm -rf "$out/data" "$out"/run-*.log
mkdir -p "$out/bin" "$out/data" "$out/src"

has_benches() {
  [[ $1 == "." ]] && return 0
  git -C "$repo" ls-tree -r --name-only "$1" -- benches crates \
    | grep -Eq '(^|/)benches/[^/]+\.rs$'
}

# Materializes one side's sources under src/<side> and prints that path: a
# detached worktree for a revision, or a copy of the tracked and unignored
# files for `.`, so the user's checkout is never touched.
checkout() {
  local side=$1 ref=$2
  local dir="$out/src/$side"
  if [[ $ref == "." ]]; then
    mkdir -p "$dir"
    git -C "$repo" ls-files -z --cached --others --exclude-standard \
      | python3 -I -c '
import os, shutil, sys
source, dest = sys.argv[1], sys.argv[2]
for name in filter(None, sys.stdin.buffer.read().decode().split("\0")):
    path = os.path.join(source, name)
    if os.path.lexists(path):  # skip tracked files deleted in the checkout
        os.makedirs(os.path.dirname(os.path.join(dest, name)), exist_ok=True)
        shutil.copy2(path, os.path.join(dest, name), follow_symlinks=False)
' "$repo" "$dir"
  else
    git -C "$repo" worktree add --detach --force "$dir" "$ref" >/dev/null
  fi
  echo "$dir"
}

# Builds one side's bench targets and copies the executables to bin/<side>.
#
# Both sides share one target directory so dependencies build once. Cargo
# names workspace artifacts identically whatever directory the sources sit in
# and decides freshness by mtime, so a side whose sources are older than the
# other side's artifacts would be skipped as up to date. Touching the copy's
# sources first forces its own workspace crates to rebuild.
build() {
  local side=$1 src=$2
  echo "==> Building $side benchmarks ($src)" >&2
  find "$src" -name target -prune -o -type f -name '*.rs' -exec touch {} +
  (cd "$src" && cargo bench --workspace --bench '*' --no-run --locked \
    --message-format=json-render-diagnostics) >"$out/build-$side.jsonl"
  mkdir -p "$out/bin/$side"
  python3 -I - "$out/build-$side.jsonl" "$out/bin/$side" <<'PY'
import json, shutil, sys, pathlib
dest = pathlib.Path(sys.argv[2])
for line in open(sys.argv[1], encoding="utf-8"):
    message = json.loads(line)
    target = message.get("target") or {}
    if message.get("reason") == "compiler-artifact" and "bench" in target.get("kind", []) and message.get("executable"):
        shutil.copy2(message["executable"], dest / target["name"])
PY
}

if ! has_benches "$base_ref"; then
  echo "Base revision $base_ref has no benchmarks; nothing to compare." >&2
  exit 0
fi

base_src=$(checkout base "$base_ref")
head_src=$(checkout head "$head_ref")
build base "$base_src"
build head "$head_src"

run_side() {
  local side=$1 round=$2
  for exe in "$out/bin/$side"/*; do
    [[ -x $exe ]] || continue
    echo "==> Round $round: $side $(basename "$exe")" >&2
    # A fresh Criterion home per round, so each estimate is that round's own.
    CRITERION_HOME="$out/data/$side/round-$round" "$exe" --bench --noplot \
      --warm-up-time "$warmup" --measurement-time "$measurement" \
      ${filter:+"$filter"} >>"$out/run-$side.log" 2>&1
  done
}

for ((round = 1; round <= rounds; round++)); do
  # Alternate the order so thermal or background drift hits both sides.
  if ((round % 2)); then
    run_side base "$round"
    run_side head "$round"
  else
    run_side head "$round"
    run_side base "$round"
  fi
done

python3 -I "$script_dir/compare.py" \
  --base "$out/data/base" \
  --head "$out/data/head" \
  --threshold "$threshold" \
  --report "$out/report.md"

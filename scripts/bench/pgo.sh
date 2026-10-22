#!/usr/bin/env bash
# Profile-guided optimization (PGO) build of the app and daemon (#30).
#
#   scripts/bench/pgo.sh                  # non-GUI training, then PGO binaries
#   PGO_GUI=1 scripts/bench/pgo.sh        # also train the app through the
#                                         # bench harness (opens windows)
#   PGO_BENCHES=1 scripts/bench/pgo.sh    # also build PGO Criterion benches
#                                         # for an A/B against plain release
#
# 1. Instrument: build with `-Cprofile-generate` into $PGO_DIR/instrumented.
#    The build passes `--target <host>`, so RUSTFLAGS reach only target code;
#    build scripts and proc macros stay uninstrumented and write no profiles
#    of the build itself (the same trick `cargo pgo` uses).
# 2. Train. The default training set needs no window:
#    - the Criterion benches (`benches/desktop.rs`, `wire`, `persistence`):
#      Markdown streaming, highlighting, review-diff parsing, the terminal
#      snapshot, wire encode/decode and the daemon's task-state saves;
#    - every built-in timeline replayed by `waku-bench-daemon --autoplay` to
#      `replay-client`, the desktop's launch-and-stream path without a
#      window: orchestrator, provider session manager and driver (fed by the
#      fake provider CLI), v8 server and streams, and the client's projection
#      store, reducers and render views;
#    - `rpc-latency` against the real `waku-daemon`: v8 handshake,
#      settings, thread snapshot, the `@` file index and thread search.
#    With PGO_GUI=1 it also runs `scripts/bench/harness.py` against the
#    instrumented app (built with `perf-counters`, which the harness needs)
#    for the start, idle and streaming scenarios. That opens windows: run it
#    on an otherwise idle machine. GUI training is what covers GPUI layout and
#    the transcript's render path; the benches cover only window-free code.
# 3. Merge the raw profiles with `llvm-profdata` from rustc's own LLVM.
# 4. Build `waku`, `waku_js_repl` and `waku-daemon` with `-Cprofile-use` into
#    $PGO_DIR/release (the release profile, so thin LTO as usual). Functions
#    the training never ran are optimized as before; `-pgo-warn-missing-
#    function` stays off because the training set covers only part of the
#    app.
#
# C dependencies (SQLite, mimalloc, ...) are built by the `cc` crate, which
# forwards `-Cprofile-generate`/`-Cprofile-use` to the C compiler. Their
# instrumentation must come from the same LLVM as rustc's profile runtime:
# Apple clang (an older LLVM) writes an older profile format, and every
# binary with such code then crashes at exit ("Runtime and instrumentation
# version mismatch"). So both builds compile C with a clang whose LLVM major
# matches rustc's, found next to llvm-profdata (Homebrew's `llvm` has both).
#
# Environment:
#   PGO_DIR        work directory (default: $CARGO_TARGET_DIR/pgo, or
#                  target/pgo). Instrumented builds and profiles live here;
#                  delete it when done, it holds two release target trees.
#   PGO_GUI        1 to add the GUI harness scenarios to the training
#   PGO_SCENARIOS  harness scenarios for PGO_GUI (default: start,idle,prose,
#                  burst,reasoning-fast,code-wall,tool-storm)
#   PGO_BENCHES    1 to also build the Criterion benches with the profile,
#                  into $PGO_DIR/benches, for scripts/bench/compare.py
#   PGO_PROFILE    use this merged .profdata instead of training (skips 1–3)
#   LLVM_PROFDATA  llvm-profdata to use (default: found next to rustc, then
#                  on PATH, then Homebrew's llvm; its major version must match
#                  `rustc -vV`)
#   PGO_CC         C compiler for the C dependencies (default: the clang next
#                  to llvm-profdata, or Homebrew's; same LLVM major as rustc)
#
# Results: $PGO_DIR/release/{waku,waku_js_repl,waku-daemon} and
# $PGO_DIR/merged.profdata. Nothing in the repository's own target directory
# or the release profile changes; ship the binaries only after comparing them
# against a plain release build (docs/fork/bench/baseline.md method).

set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo"

host="$(rustc -vV | sed -n 's/^host: //p')"
llvm_major="$(rustc -vV | sed -n 's/^LLVM version: \([0-9]*\).*/\1/p')"
target_dir="${CARGO_TARGET_DIR:-$repo/target}"
pgo_dir="${PGO_DIR:-$target_dir/pgo}"
mkdir -p "$pgo_dir"
pgo_dir="$(cd "$pgo_dir" && pwd)"
profiles="$pgo_dir/profiles"
merged="$pgo_dir/merged.profdata"

find_profdata() {
  if [[ -n "${LLVM_PROFDATA:-}" ]]; then
    echo "$LLVM_PROFDATA"
    return
  fi
  local candidate
  for candidate in \
    "$(rustc --print sysroot)/lib/rustlib/$host/bin/llvm-profdata" \
    "$(command -v llvm-profdata || true)" \
    "$(brew --prefix llvm 2>/dev/null || true)/bin/llvm-profdata"; do
    if [[ -x $candidate ]] \
      && "$candidate" --version 2>/dev/null | grep -Eq "LLVM version $llvm_major\."; then
      echo "$candidate"
      return
    fi
  done
  echo "no llvm-profdata for LLVM $llvm_major (rustc's); install the" \
    "llvm-tools rustup component or set LLVM_PROFDATA" >&2
  exit 2
}

find_clang() {
  if [[ -n "${PGO_CC:-}" ]]; then
    echo "$PGO_CC"
    return
  fi
  local candidate
  for candidate in \
    "$(dirname "$1")/clang" \
    "$(brew --prefix llvm 2>/dev/null || true)/bin/clang"; do
    if [[ -x $candidate ]] \
      && "$candidate" --version 2>/dev/null | grep -Eq "clang version $llvm_major\."; then
      echo "$candidate"
      return
    fi
  done
  echo "no clang for LLVM $llvm_major (rustc's) to build the C dependencies;" \
    "install Homebrew's llvm or set PGO_CC" >&2
  exit 2
}

# Waits for `NAME=value` on a daemon's stdout file and prints the value.
read_line() {
  local file=$1 name=$2 attempt
  for attempt in $(seq 1 1200); do
    if grep -q "^$name=" "$file" 2>/dev/null; then
      sed -n "s/^$name=//p" "$file" | head -n 1
      return
    fi
    sleep 0.05
  done
  echo "timed out waiting for $name in $file" >&2
  exit 1
}

# Continuous mode (`%c` below) keeps the counters in an mmapped file, so a
# process stopped with SIGTERM (the harness stops the app that way, and the
# training stops waku-daemon that way) still leaves a complete profile. On
# Darwin it needs the counter sections page-aligned.
generate_flags="-Cprofile-generate=$profiles"
if [[ "$(uname -s)" == Darwin ]]; then
  for section in __llvm_prf_cnts __llvm_prf_data __llvm_prf_bits; do
    generate_flags+=" -Clink-arg=-Wl,-sectalign,__DATA,$section,0x4000"
  done
fi

instrumented_build() {
  echo "==> building instrumented binaries" >&2
  rm -rf "$profiles"
  mkdir -p "$profiles"
  local features=()
  if [[ "${PGO_GUI:-0}" == 1 ]]; then
    features=(--features perf-counters)
  fi
  RUSTFLAGS="${RUSTFLAGS:-} $generate_flags" \
    CARGO_TARGET_DIR="$pgo_dir/instrumented" \
    cargo build --release --locked --target "$host" \
      -p waku --bin waku ${features[@]+"${features[@]}"} \
      -p waku-daemon --bin waku-daemon \
      -p waku-bench-daemon --bin waku-bench-daemon --bin rpc-latency --bin replay-client \
      --bin fake-codex --bin fake-claude
  # The benches use the release profile too, so their functions hash the
  # same as the binaries the profile is applied to.
  RUSTFLAGS="${RUSTFLAGS:-} $generate_flags" \
    CARGO_TARGET_DIR="$pgo_dir/instrumented" \
    cargo bench --locked --target "$host" --workspace --bench '*' --no-run --profile release
}

train() {
  local bins="$pgo_dir/instrumented/$host/release"
  local work="$pgo_dir/train"
  rm -rf "$work"
  mkdir -p "$work/home"
  # Profiles land in $profiles whatever HOME is; the HOME is disposable so the
  # daemons never touch real Waku data.
  export HOME="$work/home" WAKU_DISABLE_ANALYTICS=1
  export LLVM_PROFILE_FILE="$profiles/%c%p-%m.profraw"

  echo "==> training: Criterion benches" >&2
  local exe
  for exe in "$bins/deps"/{desktop,wire,persistence}-*; do
    [[ -x $exe && $exe != *.d ]] || continue
    "$exe" --bench --noplot --warm-up-time 1 --measurement-time 2 >"$work/bench.log" 2>&1
  done

  echo "==> training: timelines through waku-bench-daemon and replay-client" >&2
  local timeline out pid
  for timeline in prose burst reasoning-fast code-wall tool-storm images; do
    out="$work/bench-daemon-$timeline.out"
    # Not `--exit-after`: the daemon would exit as soon as the run ends,
    # possibly before the client has drawn its last view. Stop it once the
    # client has seen the turn settle; SIGTERM shuts it down gracefully.
    "$bins/waku-bench-daemon" --timeline "$timeline" --rate max --autoplay=100 \
      --history-sessions 20 --history-turns 50 \
      --state-dir "$work/bench-state-$timeline" >"$out" 2>"$work/bench-daemon-$timeline.err" &
    pid=$!
    WAKU_DAEMON_ADDRESS="$(read_line "$out" WAKU_DAEMON_ADDRESS)" \
      WAKU_DAEMON_TOKEN="$(read_line "$out" WAKU_DAEMON_TOKEN)" \
      "$bins/replay-client" --thread "$(read_line "$out" WAKU_BENCH_THREAD_ID)" \
      --timeout 120 >>"$work/replay.jsonl"
    kill -TERM "$pid"
    wait "$pid" || true
  done

  echo "==> training: rpc-latency against waku-daemon" >&2
  out="$work/daemon.out"
  local token=pgo-training-token
  WAKU_DAEMON_TOKEN=$token "$bins/waku-daemon" --bind 127.0.0.1:0 --parent-pid $$ \
    >"$out" 2>"$work/daemon.err" &
  pid=$!
  local address
  address="$(read_address "$out")"
  # Outside any Git checkout (the work directory usually sits in an ignored
  # target/), or the project file listing sees every file as ignored.
  local project
  project="$(mktemp -d)"
  "$bins/rpc-latency" --address "$address" --token "$token" --count 300 --warmup 20 \
    --project "$project/rpc-project" --out "$work/rpc-latency.json" >/dev/null
  rm -rf "$project"
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true

  if [[ "${PGO_GUI:-0}" == 1 ]]; then
    echo "==> training: GUI harness (opens windows)" >&2
    python3 scripts/bench/harness.py run --bins "$bins" --out "$work/harness" --rounds 1 \
      --scenarios "${PGO_SCENARIOS:-start,idle,prose,burst,reasoning-fast,code-wall,tool-storm}"
  fi
  unset LLVM_PROFILE_FILE
}

# `waku-daemon` prints one JSON ready line holding its bound address.
read_address() {
  local file=$1 attempt
  for attempt in $(seq 1 1200); do
    if [[ -s $file ]]; then
      python3 -I -c 'import json,sys; print(json.loads(open(sys.argv[1]).readline())["address"])' "$file"
      return
    fi
    sleep 0.05
  done
  echo "timed out waiting for waku-daemon's ready line" >&2
  exit 1
}

profdata="$(find_profdata)"
clang="$(find_clang "$profdata")"
export CC="$clang" CXX="$clang++"
if [[ -n "${PGO_PROFILE:-}" ]]; then
  merged="$PGO_PROFILE"
else
  instrumented_build
  train
  echo "==> merging $(find "$profiles" -name '*.profraw' | wc -l | tr -d ' ') raw profiles" >&2
  "$profdata" merge -o "$merged" "$profiles"
fi

echo "==> building with the profile" >&2
# Expect "Inconsistent number of value sites ... stale profile" warnings:
# this LLVM emits them even for a binary built from the very source it was
# trained on, and they only mean those value profiles are skipped.
use_flags="-Cprofile-use=$merged"
RUSTFLAGS="${RUSTFLAGS:-} $use_flags" CARGO_TARGET_DIR="$pgo_dir/use" \
  cargo build --release --locked --target "$host" \
    -p waku --bin waku --bin waku_js_repl -p waku-daemon --bin waku-daemon
mkdir -p "$pgo_dir/release"
cp "$pgo_dir/use/$host/release/"{waku,waku_js_repl,waku-daemon} "$pgo_dir/release/"

if [[ "${PGO_BENCHES:-0}" == 1 ]]; then
  echo "==> building Criterion benches with the profile" >&2
  RUSTFLAGS="${RUSTFLAGS:-} $use_flags" CARGO_TARGET_DIR="$pgo_dir/use" \
    cargo bench --locked --target "$host" --workspace --bench '*' --no-run --profile release \
      --message-format=json-render-diagnostics >"$pgo_dir/benches.jsonl"
  mkdir -p "$pgo_dir/benches"
  python3 -I - "$pgo_dir/benches.jsonl" "$pgo_dir/benches" <<'PY'
import json, pathlib, shutil, sys
dest = pathlib.Path(sys.argv[2])
for line in open(sys.argv[1], encoding="utf-8"):
    message = json.loads(line)
    target = message.get("target") or {}
    if message.get("reason") == "compiler-artifact" and "bench" in target.get("kind", []) and message.get("executable"):
        shutil.copy2(message["executable"], dest / target["name"])
PY
fi

ls -l "$pgo_dir/release"
echo "profile: $merged" >&2

#!/usr/bin/env bash
# Build the fake provider CLIs and expose them as `codex` and `claude` in a
# private PATH directory, so a benchmark runner can drive the real Waku daemon
# or T3 Code with scripted timelines and no network (issue #8).
#
#   scripts/bench/fake-path.sh [--dir DIR] [--timeline NAME|PATH] [--rate FACTOR|max]
#                              [--loops N] [--log FILE] [--debug]
#
# The directory gets `codex` and `claude` symlinks plus a `waku-bench.json` the
# fakes read on every launch. The script prints the directory on stdout, so:
#
#   PATH="$(scripts/bench/fake-path.sh --timeline burst):$PATH" <app>
#
# Only the runner's own environment should see this PATH. Env vars
# WAKU_BENCH_TIMELINE, WAKU_BENCH_RATE, WAKU_BENCH_LOOPS and WAKU_BENCH_LOG
# override the file per process. See crates/waku-bench-daemon/README.md.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
dir="${TMPDIR:-/tmp}/waku-bench-path"
timeline="prose"
rate="1"
loops="1"
log=""
profile="release"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --dir) dir="$2"; shift 2 ;;
    --timeline) timeline="$2"; shift 2 ;;
    --rate) rate="$2"; shift 2 ;;
    --loops) loops="$2"; shift 2 ;;
    --log) log="$2"; shift 2 ;;
    --debug) profile="debug"; shift ;;
    -h|--help) sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "fake-path.sh: unknown argument $1" >&2; exit 2 ;;
  esac
done

# A timeline given as a file must survive the fakes' changed working directory.
if [ -f "$timeline" ]; then
  timeline="$(cd "$(dirname "$timeline")" && pwd)/$(basename "$timeline")"
fi
case "$loops" in
  ''|*[!0-9]*) echo "fake-path.sh: --loops must be a positive integer" >&2; exit 2 ;;
esac

cd "$root"
target_dir="${CARGO_TARGET_DIR:-$root/target}"
if [ "$profile" = "release" ]; then
  cargo build --locked --release --package waku-bench-daemon \
    --bin fake-codex --bin fake-claude >&2
else
  cargo build --locked --package waku-bench-daemon \
    --bin fake-codex --bin fake-claude >&2
fi

exe_suffix=""
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) exe_suffix=".exe" ;;
esac

mkdir -p "$dir"
for pair in "codex:fake-codex" "claude:fake-claude"; do
  name="${pair%%:*}"
  binary="$target_dir/$profile/${pair#*:}$exe_suffix"
  if [ ! -x "$binary" ]; then
    echo "fake-path.sh: $binary was not built" >&2
    exit 1
  fi
  ln -sf "$binary" "$dir/$name$exe_suffix"
done

json_string() {
  # Minimal JSON string escaping for paths and names.
  printf '"%s"' "$(printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g')"
}

{
  printf '{"timeline": %s, "rate": %s, "loops": %s' \
    "$(json_string "$timeline")" "$(json_string "$rate")" "$loops"
  if [ -n "$log" ]; then
    printf ', "log": %s' "$(json_string "$log")"
  fi
  printf '}\n'
} > "$dir/waku-bench.json"

echo "fake-path.sh: $dir → codex, claude ($timeline at rate $rate, loops $loops)" >&2
printf '%s\n' "$dir"

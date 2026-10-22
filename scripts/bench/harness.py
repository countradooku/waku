#!/usr/bin/env python3
"""Waku baseline performance harness (issue #9).

Drives the release desktop app (built with `--features perf-counters`)
against `waku-bench-daemon` (the real daemon composition over protocol v8,
with `fake-codex` / `fake-claude` as its provider CLIs), and records
per-process CPU, wakeups and memory from outside plus the app's own perf
counters from inside. The daemon's process tree is split into the daemon
itself and the provider CLIs it runs (`daemon` and `providers`).
`scripts/bench/baseline.sh` builds the binaries and calls this script; run it
directly to repeat one scenario.

    harness.py run --out DIR [--rounds N] [--scenarios a,b,...]
    harness.py summarize --out DIR [--raw FILE] [--markdown FILE]
    harness.py measure --pid PID[,PID] --seconds S [--label L] [--json FILE]

Scenarios: start, idle, prose, burst, reasoning-fast, code-wall, tool-storm,
memory-1, memory-5, memory-20, rpc.

Isolation: every app and daemon process gets HOME set to a directory under
--out, so the app's `~/.waku-fork/app.json` and its
`~/Library/Application Support/Waku Fork/state.json` land there; the bench
daemon keeps its data in the run's `daemon-state`. Only processes this script
spawned are ever signalled. macOS-only (proc_pid_rusage, footprint).
"""

from __future__ import annotations

import argparse
import ctypes
import ctypes.util
import json
import os
import platform
import re
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
# Project directories live outside the checkout: inside it, `git` and the
# file walkers would see the Waku repository (and its .gitignore of temp/).
PROJECT_ROOT = Path(tempfile.gettempdir()) / f"waku-bench-projects-{os.getpid()}"
SAMPLE_INTERVAL = 0.5
STREAM_TIMELINES = ["prose", "burst", "reasoning-fast", "code-wall", "tool-storm"]
ALL_SCENARIOS = (
    ["start", "idle"]
    + STREAM_TIMELINES
    + ["memory-1", "memory-5", "memory-20", "rpc"]
)
# Release builds keep app state under these names (waku_protocol::identity).
DATA_DIRECTORY_NAME = "Waku Fork"
APP_STATE_VERSION = 1

# ── proc_pid_rusage ──────────────────────────────────────────────────────────

_RUSAGE_FIELDS = [
    "user_time", "system_time", "pkg_idle_wkups", "interrupt_wkups", "pageins",
    "wired_size", "resident_size", "phys_footprint", "proc_start_abstime",
    "proc_exit_abstime", "child_user_time", "child_system_time",
    "child_pkg_idle_wkups", "child_interrupt_wkups", "child_pageins",
    "child_elapsed_abstime", "diskio_bytesread", "diskio_byteswritten",
    "cpu_time_qos_default", "cpu_time_qos_maintenance", "cpu_time_qos_background",
    "cpu_time_qos_utility", "cpu_time_qos_legacy", "cpu_time_qos_user_initiated",
    "cpu_time_qos_user_interactive", "billed_system_time", "serviced_system_time",
    "logical_writes", "lifetime_max_phys_footprint", "instructions", "cycles",
    "billed_energy", "serviced_energy", "interval_max_phys_footprint",
    "runnable_time",
]


class _RusageInfoV4(ctypes.Structure):
    _fields_ = [("uuid", ctypes.c_uint8 * 16)] + [
        (name, ctypes.c_uint64) for name in _RUSAGE_FIELDS
    ]


class _Timebase(ctypes.Structure):
    _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]


_LIBSYSTEM = ctypes.CDLL(ctypes.util.find_library("System"))
_TIMEBASE = _Timebase()
_LIBSYSTEM.mach_timebase_info(ctypes.byref(_TIMEBASE))
RUSAGE_INFO_V4 = 4


def _abs_to_ns(ticks: int) -> float:
    return ticks * _TIMEBASE.numer / _TIMEBASE.denom


def rusage(pid: int) -> dict | None:
    """One process's counters: CPU (incl. reaped children), wakeups, memory."""
    info = _RusageInfoV4()
    if _LIBSYSTEM.proc_pid_rusage(pid, RUSAGE_INFO_V4, ctypes.byref(info)) != 0:
        return None
    return {
        "cpu_ns": _abs_to_ns(
            info.user_time + info.system_time
            + info.child_user_time + info.child_system_time
        ),
        "intr_wakeups": info.interrupt_wkups + info.child_interrupt_wkups,
        "idle_wakeups": info.pkg_idle_wkups + info.child_pkg_idle_wkups,
        "rss": info.resident_size,
        "footprint": info.phys_footprint,
        "lifetime_max_footprint": info.lifetime_max_phys_footprint,
    }


def descendants(root: int) -> list[int]:
    out = subprocess.run(
        ["ps", "-axo", "pid=,ppid="], capture_output=True, text=True, check=False
    ).stdout
    children: dict[int, list[int]] = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) == 2:
            children.setdefault(int(parts[1]), []).append(int(parts[0]))
    found, stack = [], [root]
    while stack:
        for child in children.get(stack.pop(), []):
            found.append(child)
            stack.append(child)
    return found


PROVIDER_NAMES = {"codex", "claude", "fake-codex", "fake-claude"}


def process_table() -> tuple[dict[int, list[int]], dict[int, str]]:
    out = subprocess.run(
        ["ps", "-axo", "pid=,ppid=,comm="], capture_output=True, text=True, check=False
    ).stdout
    children: dict[int, list[int]] = {}
    names: dict[int, str] = {}
    for line in out.splitlines():
        parts = line.split(None, 2)
        if len(parts) == 3:
            pid, ppid = int(parts[0]), int(parts[1])
            children.setdefault(ppid, []).append(pid)
            names[pid] = Path(parts[2]).name
    return children, names


def split_tree(root: int, table=None) -> tuple[list[int], list[int]]:
    """A process tree split into the shell (root and everything that is not
    a provider CLI) and provider CLI subtrees (codex/claude, real or fake).
    `table` is a `process_table()` to reuse."""
    children, names = table or process_table()
    shell, providers = [root], []
    stack = [(root, False)]
    while stack:
        pid, in_provider = stack.pop()
        for child in children.get(pid, []):
            provider = in_provider or names.get(child, "") in PROVIDER_NAMES
            (providers if provider else shell).append(child)
            stack.append((child, provider))
    return shell, providers


def sum_samples(pids: list[int]) -> dict:
    total = {"cpu_ns": 0.0, "intr_wakeups": 0, "idle_wakeups": 0, "rss": 0,
             "footprint": 0, "processes": 0, "t": time.monotonic()}
    for pid in pids:
        sample = rusage(pid)
        if sample is None:
            continue
        total["processes"] += 1
        for key in ("cpu_ns", "intr_wakeups", "idle_wakeups", "rss", "footprint"):
            total[key] += sample[key]
    return total


def tree_sample(root: int, part: str = "all", table=None) -> dict | None:
    """Sum of `rusage` over a process and its live descendants.

    `part` is "all" (the whole tree), "shell" (without the provider CLI
    subtrees) or "providers" (only those), as `split_tree` divides it.
    `table` is a `process_table()` to reuse across samples of one instant."""
    base = rusage(root)
    if base is None:
        return None
    if part != "all":
        shell, providers = split_tree(root, table)
        return sum_samples(shell if part == "shell" else providers)
    total = dict(base)
    total["processes"] = 1
    if table:
        shell, providers = split_tree(root, table)
        pids = shell[1:] + providers  # shell[0] is the root
    else:
        pids = descendants(root)
    for pid in pids:
        sample = rusage(pid)
        if sample is None:
            continue
        total["processes"] += 1
        for key in ("cpu_ns", "intr_wakeups", "idle_wakeups", "rss", "footprint"):
            total[key] += sample[key]
    total["t"] = time.monotonic()
    return total


def footprint_tool(pid: int) -> float | None:
    """`footprint -p` total in MB (the same phys_footprint, via the CLI)."""
    try:
        out = subprocess.run(
            ["footprint", "-p", str(pid)], capture_output=True, text=True, timeout=30
        ).stdout
    except (OSError, subprocess.TimeoutExpired):
        return None
    match = re.search(r"Footprint:\s*([\d.]+)\s*(KB|MB|GB|B)", out)
    if not match:
        return None
    scale = {"B": 1 / 1024 / 1024, "KB": 1 / 1024, "MB": 1, "GB": 1024}[match.group(2)]
    return float(match.group(1)) * scale


_WINDOW_LIST_JS = """
ObjC.import('CoreGraphics');
var list = ObjC.deepUnwrap(ObjC.castRefToObject(
    $.CGWindowListCopyWindowInfo($.kCGWindowListOptionOnScreenOnly, 0)));
JSON.stringify(list.filter(w => w.kCGWindowLayer == 0 && w.kCGWindowAlpha > 0)
    .map(w => [w.kCGWindowOwnerPID, w.kCGWindowBounds.X, w.kCGWindowBounds.Y,
               w.kCGWindowBounds.Width, w.kCGWindowBounds.Height]))
"""


def visible_fraction(pid: int) -> float | None:
    """How much of `pid`'s largest window is on screen and not covered by
    windows in front of it (0..1), from window bounds (no screen-recording
    permission needed). An occluded GPUI window draws no frames, which turns
    a streaming run into a background run. Approximate: it ignores Spaces
    and transparency, so it is recorded, not used to discard runs."""
    try:
        out = subprocess.run(["osascript", "-l", "JavaScript", "-e", _WINDOW_LIST_JS],
                             capture_output=True, text=True, timeout=10).stdout
        windows = json.loads(out)
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return None
    # Front to back. The app's main window is its largest.
    mine = [(i, w) for i, w in enumerate(windows) if w[0] == pid]
    if not mine:
        return 0.0
    index, (_, x, y, width, height) = max(mine, key=lambda item: item[1][3] * item[1][4])
    above = [w for w in windows[:index] if w[0] != pid]
    steps, covered = 24, 0
    for i in range(steps):
        for j in range(steps):
            px = x + (i + 0.5) * width / steps
            py = y + (j + 0.5) * height / steps
            if any(ax <= px < ax + aw and ay <= py < ay + ah for _, ax, ay, aw, ah in above):
                covered += 1
    return 1 - covered / (steps * steps)


def bench_sample(app_pid: int, daemon_pid: int) -> dict:
    """The app's tree, the daemon without its provider CLIs, and those CLIs."""
    table = process_table()
    return {
        "app": tree_sample(app_pid, "all", table),
        "daemon": tree_sample(daemon_pid, "shell", table),
        "providers": tree_sample(daemon_pid, "providers", table),
    }


def bench_roles(app_pid: int, daemon_pid: int) -> dict[str, tuple[int, str]]:
    return {"app": (app_pid, "all"), "daemon": (daemon_pid, "shell"),
            "providers": (daemon_pid, "providers")}


class Sampler(threading.Thread):
    """Polls process-tree samples every SAMPLE_INTERVAL for peak memory.

    `roles` maps a role to `(root pid, part)` (see `tree_sample`)."""

    def __init__(self, roles: dict[str, tuple[int, str]]):
        super().__init__(daemon=True)
        self.roles = roles
        self.samples: dict[str, list[dict]] = {role: [] for role in roles}
        self.visibility: list[tuple[float, float]] = []
        self._halt = threading.Event()

    def run(self) -> None:
        tick = 0
        while not self._halt.is_set():
            table = process_table()
            for role, (pid, part) in self.roles.items():
                sample = tree_sample(pid, part, table)
                if sample:
                    self.samples[role].append(sample)
            if "app" in self.roles and tick % 2 == 0:
                fraction = visible_fraction(self.roles["app"][0])
                if fraction is not None:
                    self.visibility.append((time.monotonic(), fraction))
            tick += 1
            self._halt.wait(SAMPLE_INTERVAL)

    def visible(self, since: float, until: float) -> float | None:
        """Mean visible fraction of the app window in [since, until]."""
        values = [f for t, f in self.visibility if since <= t <= until]
        return round(sum(values) / len(values), 3) if values else None

    def stop(self) -> None:
        self._halt.set()
        self.join(timeout=5)

    def peak(self, role: str, key: str, since: float = 0.0) -> float:
        values = [s[key] for s in self.samples[role] if s["t"] >= since]
        return max(values) if values else 0.0


# ── Process management ───────────────────────────────────────────────────────

SPAWNED: list[subprocess.Popen] = []


def spawn(args, **kwargs) -> subprocess.Popen:
    process = subprocess.Popen(args, **kwargs)
    SPAWNED.append(process)
    return process


def stop(process: subprocess.Popen, grace: float = 5.0) -> None:
    """SIGTERM, then SIGKILL, a process this script spawned."""
    if process.poll() is None:
        try:
            process.send_signal(signal.SIGTERM)
            process.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=grace)
        except ProcessLookupError:
            pass
    if process in SPAWNED:
        SPAWNED.remove(process)


def stop_all() -> None:
    for process in list(SPAWNED):
        stop(process, grace=2.0)


def clean_env(home: Path, extra: dict[str, str]) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("WAKU_") and key not in ("CODEX_HOME", "CLAUDE_CONFIG_DIR")
    }
    env["HOME"] = str(home)
    env["WAKU_DISABLE_ANALYTICS"] = "1"
    env.update(extra)
    return env


def wait_for(predicate, timeout: float, interval: float = 0.1, what: str = "condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise TimeoutError(f"timed out after {timeout:.0f}s waiting for {what}")


def read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


class BenchDaemon:
    def __init__(self, bins: Path, run_dir: Path, home: Path, args: list[str]):
        self.log_path = run_dir / "daemon.log"
        project = PROJECT_ROOT / run_dir.parent.name / run_dir.name
        if project.exists():
            shutil.rmtree(project)
        project.mkdir(parents=True)
        (project / "README.md").write_text("# Bench project\n")
        self.project = project
        self.process = spawn(
            [
                str(bins / "waku-bench-daemon"), *args,
                "--state-dir", str(run_dir / "daemon-state"),
                "--project", str(project),
                "--fakes", str(bins),
            ],
            stdout=subprocess.PIPE,
            stderr=open(self.log_path, "w"),
            text=True,
            env=clean_env(home, {}),
        )
        self.ids: dict[str, str] = {}
        assert self.process.stdout is not None
        while "WAKU_DAEMON_ADDRESS" not in self.ids or "WAKU_DAEMON_TOKEN" not in self.ids \
                or "WAKU_BENCH_THREAD_ID" not in self.ids:
            line = self.process.stdout.readline()
            if not line:
                raise RuntimeError(f"bench daemon exited; see {self.log_path}")
            if "=" in line:
                key, value = line.strip().split("=", 1)
                self.ids[key] = value
        # Further stdout (the exit summary) is drained by a thread.
        self.summary: list[str] = []
        threading.Thread(target=self._drain, daemon=True).start()
        self.pid = self.process.pid

    def _drain(self) -> None:
        assert self.process.stdout is not None
        for line in self.process.stdout:
            line = line.rstrip("\n")
            if line.startswith("WAKU_BENCH_") and "=" in line:
                key, value = line.split("=", 1)
                self.ids[key] = value
            else:
                self.summary.append(line)

    def log(self) -> str:
        try:
            return self.log_path.read_text()
        except OSError:
            return ""

    def wait_log(self, needle: str, timeout: float) -> float:
        wait_for(lambda: needle in self.log(), timeout, what=f"daemon log {needle!r}")
        return time.monotonic()

    def replay_line(self) -> dict:
        match = re.search(
            r"(\S+) replay (finished|canceled) · (\d+) events in ([\d.]+)s "
            r"\((\d+)/s, max lag ([\d.]+) ms\)",
            self.log(),
        )
        if not match:
            return {}
        return {
            "timeline": match.group(1),
            "status": match.group(2),
            "events": int(match.group(3)),
            "elapsed_s": float(match.group(4)),
            "max_lag_ms": float(match.group(6)),
        }


def provider_dirs(home: Path) -> list[str]:
    """Provider config directories a run created: a real CLI was started."""
    return [name for name in (".codex", ".claude", ".claude.json") if (home / name).exists()]


def write_app_state(home: Path, project_id: str, thread_id: str) -> None:
    """Preselect a thread the way a returning user's state.json would.

    The desktop keeps its selection in its own UI state
    (`AppState` in crates/waku-client/src/persistence.rs, written to
    `state.json` beside its data directory): `selected_project` is the
    orchestration project id and `selected_session` the thread id, which
    the launch opens first (`threads.open_thread`, src/startup.rs) and so
    subscribes to its `thread` stream; that subscription is what
    `--autoplay` waits for."""
    directory = home / "Library" / "Application Support" / DATA_DIRECTORY_NAME
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "state.json"
    state = read_json(path) or {"app_state_version": APP_STATE_VERSION}
    state["selected_project"] = project_id
    state["selected_session"] = thread_id
    path.write_text(json.dumps(state))


def bring_to_front(pid: int) -> None:
    """Raise the app's window: a covered GPUI window draws nothing, and the
    run is then discarded. Best effort (macOS only, needs Automation access
    to System Events)."""
    if sys.platform != "darwin":
        return
    subprocess.run(
        ["osascript", "-e",
         f'tell application "System Events" to set frontmost of '
         f'(first process whose unix id is {pid}) to true'],
        capture_output=True, timeout=10, check=False,
    )


class App:
    def __init__(self, bins: Path, run_dir: Path, home: Path, daemon: BenchDaemon,
                 extra_env: dict[str, str] | None = None):
        self.counters_path = run_dir / "counters.json"
        self.counters_path.unlink(missing_ok=True)
        env = clean_env(home, {
            "WAKU_DAEMON_ADDRESS": daemon.ids["WAKU_DAEMON_ADDRESS"],
            "WAKU_DAEMON_TOKEN": daemon.ids["WAKU_DAEMON_TOKEN"],
            "WAKU_PERF_COUNTERS_OUT": str(self.counters_path),
            **(extra_env or {}),
        })
        self.spawned_at = time.monotonic()
        self.process = spawn(
            [str(bins / "waku")],
            cwd=str(daemon.project),
            env=env,
            stdout=open(run_dir / "app.log", "w"),
            stderr=subprocess.STDOUT,
        )
        self.pid = self.process.pid

    def counters(self) -> dict | None:
        return read_json(self.counters_path)

    def wait_started(self, key: str = "first_frame_ms", timeout: float = 60) -> dict:
        def ready():
            if self.process.poll() is not None:
                raise RuntimeError("the app exited during startup")
            report = self.counters()
            return report if report and report.get("startup", {}).get(key) is not None else None

        report = wait_for(ready, timeout, what=f"app {key}")
        bring_to_front(self.pid)
        return report

    def fresh_counters(self, after: float) -> dict:
        """The first report written after monotonic time `after`."""
        target = None

        def newer():
            nonlocal target
            report = self.counters()
            if report is None:
                return None
            if target is None:
                target = report["elapsed_ms"]
                return None
            return report if report["elapsed_ms"] > target else None

        return wait_for(newer, 5, interval=0.05, what="a fresh counters report")


# ── Metrics ──────────────────────────────────────────────────────────────────

MB = 1024 * 1024


def process_window(a: dict, b: dict) -> dict:
    seconds = max(b["t"] - a["t"], 1e-9)
    cpu_s = (b["cpu_ns"] - a["cpu_ns"]) / 1e9
    return {
        "window_s": round(seconds, 2),
        "cpu_s": round(cpu_s, 3),
        "cpu_pct": round(100 * cpu_s / seconds, 2),
        "intr_wakeups_per_s": round((b["intr_wakeups"] - a["intr_wakeups"]) / seconds, 1),
        "idle_wakeups_per_s": round((b["idle_wakeups"] - a["idle_wakeups"]) / seconds, 1),
        "processes": b.get("processes", 1),
    }


def memory(sampler: Sampler, role: str, final: dict, pid: int | None,
           since: float = 0.0) -> dict:
    """Peak and final memory of a role. `pid` is the role's root process
    (its lifetime peak and `footprint -p`); None for the provider CLIs,
    which have no single root."""
    root = (rusage(pid) if pid else None) or {}
    return {
        "rss_peak_mb": round(max(sampler.peak(role, "rss", since), final["rss"]) / MB, 1),
        "rss_final_mb": round(final["rss"] / MB, 1),
        "footprint_peak_mb": round(
            max(sampler.peak(role, "footprint", since), final["footprint"],
                root.get("lifetime_max_footprint", 0)) / MB, 1),
        "footprint_final_mb": round(final["footprint"] / MB, 1),
        "footprint_tool_mb": footprint_tool(pid) if pid else None,
    }


def counters_window(a: dict, b: dict) -> dict:
    """Rates between two perf-counter reports, from their own clocks."""
    seconds = max((b["elapsed_ms"] - a["elapsed_ms"]) / 1000, 1e-9)
    ta, tb = a["totals"], b["totals"]

    def delta(*path):
        x, y = ta, tb
        for key in path:
            x, y = x[key], y[key]
        return y - x

    commits = delta("stream_commits")
    commit_frames = delta("commit_frames")
    buckets = [
        bucket for bucket in b.get("series", [])
        if a["elapsed_ms"] < bucket["t_ms"] <= b["elapsed_ms"]
    ]
    return {
        "window_s": round(seconds, 2),
        "frames_per_s": round(delta("frames") / seconds, 2),
        "commits_per_s": round(commits / seconds, 2),
        "peak_commits_per_s": max((x["stream_commits"] for x in buckets), default=0),
        "peak_frames_per_s": max((x["frames"] for x in buckets), default=0),
        "renders_per_s": {
            pane: round(delta("renders", pane) / seconds, 2)
            for pane in ("transcript", "sidebar", "right_panel", "composer")
        },
        "renders": {
            pane: delta("renders", pane)
            for pane in ("transcript", "sidebar", "right_panel", "composer")
        },
        "commit_frames": commit_frames,
        # Commits that reached a drawn frame. A window that is hidden or fully
        # occluded draws nothing while the stream still commits.
        "presented_commit_ratio": round(commit_frames / commits, 3) if commits else None,
        "transcript_renders_per_commit": round(delta("renders", "transcript") / commits, 3)
        if commits else None,
        "rows_per_commit_frame": round(
            delta("transcript_rows_built_in_commit_frames") / commit_frames, 2)
        if commit_frames else None,
        "ui_alloc_kb_per_commit": round(delta("commit_alloc_bytes") / commits / 1024, 1)
        if commits else None,
        "alloc_mb_per_s": round(delta("alloc_bytes") / seconds / MB, 2),
        "save_calls": delta("save_calls"),
        "save_ui_ms": round(delta("save_ui_ns") / 1e6, 2),
        "save_max_ms": round(tb["save_max_ns"] / 1e6, 2),
        # Added in #178; older reports lack them.
        "terminal_rows_rebuilt": tb.get("terminal_rows_rebuilt", 0)
        - ta.get("terminal_rows_rebuilt", 0),
        "terminal_grid_updates": tb.get("terminal_grid_updates", 0)
        - ta.get("terminal_grid_updates", 0),
    }


def compact_counters(report: dict) -> dict:
    """A counters report without its per-second series, for the raw JSON."""
    return {key: value for key, value in report.items() if key != "series"}


# ── Scenarios ────────────────────────────────────────────────────────────────

class Context:
    def __init__(self, out: Path, bins: Path):
        self.out = out
        self.bins = bins
        self.start_home = out / "homes" / "start"
        self.cold_done = False


def run_dir(ctx: Context, scenario: str, round_index: int) -> Path:
    directory = ctx.out / "runs" / scenario / f"r{round_index}"
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir(parents=True)
    return directory


def fresh_home(directory: Path) -> Path:
    home = directory / "home"
    home.mkdir(parents=True, exist_ok=True)
    return home


def scenario_start(ctx: Context, round_index: int, kind: str) -> dict:
    """Launch to first content with 200 sessions in the database."""
    directory = run_dir(ctx, f"start-{kind}", round_index)
    home = ctx.start_home
    if kind == "cold" and home.exists():
        shutil.rmtree(home)
    home.mkdir(parents=True, exist_ok=True)
    daemon = BenchDaemon(ctx.bins, directory, home,
                         ["--timeline", "prose", "--history-turns", "10",
                          "--history-sessions", "200"])
    try:
        wait_for(lambda: "WAKU_BENCH_HISTORY_THREAD_ID" in daemon.ids or None, 5,
                 what="history thread id")
        write_app_state(home, daemon.ids["WAKU_BENCH_PROJECT_ID"],
                        daemon.ids["WAKU_BENCH_HISTORY_THREAD_ID"])
        app = App(ctx.bins, directory, home, daemon)
        report = app.wait_started("first_content_ms", timeout=60)
        time.sleep(1.0)
        sample = tree_sample(app.pid)
        startup = report["startup"]
        result = {
            "scenario": f"start-{kind}",
            "round": round_index,
            "provider_dirs": provider_dirs(home),
            "startup": startup,
            "app": {
                "rss_mb": round(sample["rss"] / MB, 1) if sample else None,
                "footprint_mb": round(sample["footprint"] / MB, 1) if sample else None,
            },
        }
        stop(app.process)
        return result
    finally:
        stop(daemon.process)


def scenario_idle(ctx: Context, round_index: int, settle: float = 10, window: float = 50) -> dict:
    directory = run_dir(ctx, "idle", round_index)
    home = fresh_home(directory)
    daemon = BenchDaemon(ctx.bins, directory, home, ["--timeline", "prose"])
    try:
        write_app_state(home, daemon.ids["WAKU_BENCH_PROJECT_ID"],
                        daemon.ids["WAKU_BENCH_THREAD_ID"])
        app = App(ctx.bins, directory, home, daemon)
        sampler = Sampler(bench_roles(app.pid, daemon.pid))
        sampler.start()
        app.wait_started()
        time.sleep(settle)
        a = bench_sample(app.pid, daemon.pid)
        counters_a = app.fresh_counters(time.monotonic())
        time.sleep(window)
        counters_b = app.fresh_counters(time.monotonic())
        b = bench_sample(app.pid, daemon.pid)
        sampler.stop()
        visible = sampler.visible(a["app"]["t"], b["app"]["t"])
        result = {
            "scenario": "idle",
            "round": round_index,
            "window_visible": visible,
            "provider_dirs": provider_dirs(home),
            "app": {**process_window(a["app"], b["app"]),
                    **memory(sampler, "app", b["app"], app.pid)},
            "daemon": {**process_window(a["daemon"], b["daemon"]),
                       **memory(sampler, "daemon", b["daemon"], daemon.pid)},
            "providers": {**process_window(a["providers"], b["providers"]),
                          **memory(sampler, "providers", b["providers"], None)},
            "counters": counters_window(counters_a, counters_b),
            "counters_report": compact_counters(counters_b),
        }
        stop(app.process)
        return result
    finally:
        stop(daemon.process)


def scenario_stream(ctx: Context, round_index: int, timeline: str) -> dict:
    directory = run_dir(ctx, timeline, round_index)
    home = fresh_home(directory)
    daemon = BenchDaemon(ctx.bins, directory, home,
                         ["--timeline", timeline, "--rate", "1", "--autoplay=1500"])
    try:
        write_app_state(home, daemon.ids["WAKU_BENCH_PROJECT_ID"],
                        daemon.ids["WAKU_BENCH_THREAD_ID"])
        app = App(ctx.bins, directory, home, daemon)
        sampler = Sampler(bench_roles(app.pid, daemon.pid))
        sampler.start()
        app.wait_started()
        daemon.wait_log("autoplay prompt sent", timeout=60)
        a = bench_sample(app.pid, daemon.pid)
        counters_a = app.counters()
        started = time.monotonic()
        # The daemon logs the replay line when the run that played it ends.
        wait_for(lambda: daemon.replay_line() or None, 900, interval=0.05,
                 what=f"{timeline} replay to finish")
        b = bench_sample(app.pid, daemon.pid)
        counters_b = app.fresh_counters(time.monotonic())
        # Let the last commit land and the turn settle before the final memory.
        time.sleep(3)
        final = bench_sample(app.pid, daemon.pid)
        counters_final = app.counters()
        sampler.stop()
        visible = sampler.visible(a["app"]["t"], b["app"]["t"])
        replay = daemon.replay_line()
        result = {
            "scenario": timeline,
            "round": round_index,
            "window_visible": visible,
            "provider_dirs": provider_dirs(home),
            "replay": replay,
            "wall_s": round(b["app"]["t"] - started, 2),
            "app": {**process_window(a["app"], b["app"]),
                    **memory(sampler, "app", final["app"], app.pid, since=started)},
            "daemon": {**process_window(a["daemon"], b["daemon"]),
                       **memory(sampler, "daemon", final["daemon"], daemon.pid, since=started)},
            "providers": {**process_window(a["providers"], b["providers"]),
                          **memory(sampler, "providers", final["providers"], None,
                                   since=started)},
            "counters": counters_window(counters_a, counters_b),
            "counters_report": compact_counters(counters_final or counters_b),
        }
        ratio = result["counters"]["presented_commit_ratio"]
        # The drawn-commit ratio decides. `window_visible` is informational:
        # window bounds cannot see Spaces, and a window on another Space can
        # still draw.
        result["window_drawn"] = ratio is None or ratio >= 0.8
        stop(app.process)
        return result
    finally:
        stop(daemon.process)


def scenario_memory(ctx: Context, round_index: int, sessions: int,
                    turns: int = 1000, visit_ms: int = 2500, settle: float = 20) -> dict:
    directory = run_dir(ctx, f"memory-{sessions}", round_index)
    home = fresh_home(directory)
    daemon = BenchDaemon(ctx.bins, directory, home,
                         ["--timeline", "prose", "--history-turns", str(turns),
                          "--history-sessions", str(sessions)])
    try:
        wait_for(lambda: "WAKU_BENCH_HISTORY_THREAD_ID" in daemon.ids or None, 5,
                 what="history thread id")
        write_app_state(home, daemon.ids["WAKU_BENCH_PROJECT_ID"],
                        daemon.ids["WAKU_BENCH_HISTORY_THREAD_ID"])
        app = App(ctx.bins, directory, home, daemon,
                  {"WAKU_PERF_VISIT_SESSIONS_MS": str(visit_ms)})
        sampler = Sampler(bench_roles(app.pid, daemon.pid))
        sampler.start()
        app.wait_started("first_content_ms")
        launched = tree_sample(app.pid)
        # Six timeline threads plus the history threads are visited.
        expected = len(STREAM_TIMELINES) + 1 + sessions

        def visited():
            report = app.counters()
            return report if report and report["totals"].get("session_visits", 0) >= expected else None

        wait_for(visited, expected * visit_ms / 1000 + 60, interval=0.5, what="session visits")
        time.sleep(settle)
        final = bench_sample(app.pid, daemon.pid)
        report = app.counters()
        sampler.stop()
        result = {
            "scenario": f"memory-{sessions}",
            "round": round_index,
            "provider_dirs": provider_dirs(home),
            "sessions": sessions,
            "turns_per_session": turns,
            "visits": report["totals"]["session_visits"] if report else None,
            "app_after_launch": {
                "rss_mb": round(launched["rss"] / MB, 1),
                "footprint_mb": round(launched["footprint"] / MB, 1),
            },
            "app": memory(sampler, "app", final["app"], app.pid),
            "daemon": memory(sampler, "daemon", final["daemon"], daemon.pid),
            "counters_report": compact_counters(report) if report else None,
        }
        stop(app.process)
        return result
    finally:
        stop(daemon.process)


def scenario_rpc(ctx: Context, round_index: int, count: int = 1000) -> dict:
    """Protocol v8 round trips against the bench daemon (the real daemon
    composition); `threads.snapshot` reads the prose thread, one settled
    two-message turn."""
    directory = run_dir(ctx, "rpc", round_index)
    home = fresh_home(directory)
    daemon = BenchDaemon(ctx.bins, directory, home, ["--timeline", "prose"])
    try:
        out = directory / "rpc.json"
        subprocess.run(
            [str(ctx.bins / "rpc-latency"), "--address", daemon.ids["WAKU_DAEMON_ADDRESS"],
             "--token", daemon.ids["WAKU_DAEMON_TOKEN"],
             "--thread", daemon.ids["WAKU_BENCH_THREAD_ID"],
             "--count", str(count), "--label", "waku-bench-daemon",
             "--project", str(daemon.project / "rpc-project"), "--out", str(out)],
            check=True,
            env=clean_env(home, {}),
        )
        report = json.loads(out.read_text())
        return {"scenario": "rpc", "round": round_index, **report}
    finally:
        stop(daemon.process)


def run_scenario(ctx: Context, scenario: str, round_index: int) -> dict:
    if scenario == "start":
        kind = "warm" if ctx.cold_done else "cold"
        ctx.cold_done = True
        return scenario_start(ctx, round_index, kind)
    if scenario == "idle":
        return scenario_idle(ctx, round_index)
    if scenario in STREAM_TIMELINES:
        return scenario_stream(ctx, round_index, scenario)
    if scenario.startswith("memory-"):
        return scenario_memory(ctx, round_index, int(scenario.split("-", 1)[1]))
    if scenario == "rpc":
        return scenario_rpc(ctx, round_index)
    raise ValueError(f"unknown scenario {scenario}")


def machine_info(bins: Path) -> dict:
    def run(*args):
        try:
            return subprocess.run(args, capture_output=True, text=True, timeout=30).stdout.strip()
        except OSError:
            return ""

    refresh = run(
        "osascript", "-l", "JavaScript", "-e",
        'ObjC.import("AppKit"); $.NSScreen.mainScreen.maximumFramesPerSecond',
    )
    return {
        "model": run("sysctl", "-n", "hw.model"),
        "cpu": run("sysctl", "-n", "machdep.cpu.brand_string"),
        "cores": run("sysctl", "-n", "hw.ncpu"),
        "memory_gb": round(int(run("sysctl", "-n", "hw.memsize") or 0) / 1024**3),
        "macos": f'{run("sw_vers", "-productVersion")} ({run("sw_vers", "-buildVersion")})',
        "display_max_fps": int(refresh) if refresh.isdigit() else None,
        "power_source": run("pmset", "-g", "ps").splitlines()[0] if run("pmset", "-g", "ps") else "",
        "commit": run("git", "-C", str(REPO), "rev-parse", "HEAD"),
        "dirty": bool(run("git", "-C", str(REPO), "status", "--porcelain", "--untracked-files=no")),
        "rustc": run("rustc", "--version"),
        "app_binary": str(bins / "waku"),
        "python": platform.python_version(),
    }


def command_run(args) -> None:
    out = Path(args.out).resolve()
    bins = Path(args.bins).resolve()
    scenarios = args.scenarios.split(",") if args.scenarios else ALL_SCENARIOS
    for scenario in scenarios:
        if scenario not in ALL_SCENARIOS:
            sys.exit(f"unknown scenario {scenario}; choose from {', '.join(ALL_SCENARIOS)}")
    out.mkdir(parents=True, exist_ok=True)
    if args.first_round == 1 or not (out / "machine.json").exists():
        (out / "machine.json").write_text(json.dumps(machine_info(bins), indent=2))
    ctx = Context(out, bins)
    ctx.cold_done = args.first_round > 1
    results_dir = out / "results"
    results_dir.mkdir(exist_ok=True)

    def record(result: dict) -> None:
        # Other load on the machine skews everything; keep it with the run.
        result["loadavg_1m_after"] = round(os.getloadavg()[0], 2)
        path = results_dir / f'{result["scenario"]}-r{result["round"]}.json'
        path.write_text(json.dumps(result, indent=2))
        print(f'  {result["scenario"]} r{result["round"]}: {headline(result)}', flush=True)

    # Cold start first: the first launch after the build.
    if "start" in scenarios and args.first_round == 1:
        print("start (cold)", flush=True)
        record(run_scenario(ctx, "start", 0))
    last = args.first_round + args.rounds - 1
    for round_index in range(args.first_round, last + 1):
        print(f"round {round_index}/{last}", flush=True)
        for scenario in scenarios:
            try:
                record(run_scenario(ctx, scenario, round_index))
            except Exception as error:  # noqa: BLE001 - keep the matrix going
                print(f"  {scenario} r{round_index} FAILED: {error}", flush=True)
                (results_dir / f"{scenario}-r{round_index}.error").write_text(str(error))
                stop_all()


def headline(result: dict) -> str:
    scenario = result["scenario"]
    if scenario.startswith("start"):
        s = result["startup"]
        return f'first frame {s["first_frame_ms"]:.0f} ms, first content {s["first_content_ms"]:.0f} ms'
    if scenario == "rpc":
        r = result["requests"]
        return " · ".join(f'{k} p50 {v["p50_us"]:.0f} us' for k, v in r.items())
    if scenario.startswith("memory"):
        return f'app footprint {result["app"]["footprint_final_mb"]} MB (peak {result["app"]["footprint_peak_mb"]})'
    app, daemon, counters = result["app"], result["daemon"], result.get("counters", {})
    providers = result.get("providers", {})
    return (f'app {app["cpu_pct"]}% daemon {daemon["cpu_pct"]}% '
            f'providers {providers.get("cpu_pct")}% · '
            f'{counters.get("frames_per_s")} fps · {counters.get("commits_per_s")} commits/s · '
            f'app wakeups {app["intr_wakeups_per_s"]}/s · footprint {app["footprint_final_mb"]} MB')


# ── Summary ──────────────────────────────────────────────────────────────────

def median(values):
    values = [v for v in values if isinstance(v, (int, float))]
    return round(statistics.median(values), 2) if values else None


def spread(values):
    values = [v for v in values if isinstance(v, (int, float))]
    return [round(min(values), 2), round(max(values), 2)] if len(values) > 1 else None


def get(result: dict, path: str):
    value = result
    for key in path.split("."):
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value


SUMMARY_FIELDS = {
    "start": ["startup.init_ms", "startup.first_frame_ms", "startup.first_content_ms",
              "app.rss_mb", "app.footprint_mb"],
    "idle": ["app.cpu_pct", "app.intr_wakeups_per_s", "app.idle_wakeups_per_s",
             "daemon.cpu_pct", "daemon.intr_wakeups_per_s", "providers.cpu_pct",
             "counters.frames_per_s",
             "counters.renders_per_s.transcript", "counters.renders_per_s.sidebar",
             "app.footprint_final_mb", "app.rss_final_mb", "daemon.footprint_final_mb"],
    "stream": ["replay.elapsed_s", "app.cpu_pct", "daemon.cpu_pct", "providers.cpu_pct",
               "app.cpu_s", "daemon.cpu_s", "providers.cpu_s",
               "app.intr_wakeups_per_s", "daemon.intr_wakeups_per_s",
               "counters.frames_per_s", "counters.commits_per_s", "counters.peak_commits_per_s",
               "counters.transcript_renders_per_commit", "counters.rows_per_commit_frame",
               "counters.ui_alloc_kb_per_commit", "counters.renders.sidebar",
               "counters.renders.right_panel", "counters.save_ui_ms",
               "app.rss_peak_mb", "app.footprint_peak_mb", "app.footprint_final_mb",
               "daemon.footprint_final_mb", "providers.footprint_final_mb",
               "replay.max_lag_ms"],
    "memory": ["app_after_launch.footprint_mb", "app.footprint_final_mb", "app.footprint_peak_mb",
               "app.rss_final_mb", "app.rss_peak_mb", "daemon.footprint_final_mb", "visits"],
    "rpc": [f"requests.{name}.{stat}"
            for name in ("settings_get", "threads_snapshot", "files_search_entries",
                         "threads_search")
            for stat in ("p50_us", "p95_us", "p99_us")],
}


def kind_of(scenario: str) -> str:
    if scenario.startswith("start"):
        return "start"
    if scenario.startswith("memory"):
        return "memory"
    if scenario in STREAM_TIMELINES:
        return "stream"
    return scenario


def command_measure(args) -> None:
    """Sample any process tree from outside: the cross-app (T3 Code) path.

    The shell (the root and its non-provider descendants) and provider CLI
    subtrees are reported separately. CPU and wakeups of processes that exit
    inside the window are lost unless their parent reaps them (then they
    count through the parent's child_* totals); sample long-lived trees.
    """
    roots = [int(pid) for pid in args.pid.split(",")]
    peaks = {"shell": {"rss": 0, "footprint": 0}, "providers": {"rss": 0, "footprint": 0}}
    cumulative = {"shell": {}, "providers": {}}

    def sample() -> dict:
        shell, providers = [], []
        for root in roots:
            s_pids, p_pids = split_tree(root)
            shell += s_pids
            providers += p_pids
        out = {"shell": sum_samples(shell), "providers": sum_samples(providers)}
        for role in out:
            for key in ("rss", "footprint"):
                peaks[role][key] = max(peaks[role][key], out[role][key])
        return out

    # A provider CLI that exits takes its counters with it, so CPU and
    # wakeups add up each pid's last seen value rather than diffing sums.
    per_pid_last: dict[int, dict] = {}

    def remember() -> None:
        for root in roots:
            s_pids, p_pids = split_tree(root)
            for pid in s_pids + p_pids:
                value = rusage(pid)
                if value:
                    value["role"] = "providers" if pid in p_pids else "shell"
                    per_pid_last[pid] = value

    first = sample()
    remember()
    first_cpu = {pid: dict(v) for pid, v in per_pid_last.items()}
    deadline = time.monotonic() + args.seconds
    while time.monotonic() < deadline:
        time.sleep(min(SAMPLE_INTERVAL, max(deadline - time.monotonic(), 0)))
        sample()
        remember()
    last = sample()
    seconds = max(last["shell"]["t"] - first["shell"]["t"], 1e-9)
    for role in ("shell", "providers"):
        cpu_ns = sum(
            v["cpu_ns"] - first_cpu.get(pid, {"cpu_ns": 0})["cpu_ns"]
            for pid, v in per_pid_last.items() if v["role"] == role
        )
        wakeups = sum(
            v["intr_wakeups"] - first_cpu.get(pid, {"intr_wakeups": 0})["intr_wakeups"]
            for pid, v in per_pid_last.items() if v["role"] == role
        )
        cumulative[role] = {
            "processes_seen": sum(1 for v in per_pid_last.values() if v["role"] == role),
            "cpu_s": round(cpu_ns / 1e9, 3),
            "cpu_pct": round(100 * cpu_ns / 1e9 / seconds, 2),
            "intr_wakeups_per_s": round(wakeups / seconds, 1),
            "rss_peak_mb": round(peaks[role]["rss"] / MB, 1),
            "rss_final_mb": round(last[role]["rss"] / MB, 1),
            "footprint_peak_mb": round(peaks[role]["footprint"] / MB, 1),
            "footprint_final_mb": round(last[role]["footprint"] / MB, 1),
        }
    report = {"schema": "waku.bench-measure/1", "label": args.label, "roots": roots,
              "window_s": round(seconds, 2), **cumulative}
    text = json.dumps(report, indent=2)
    if args.json:
        Path(args.json).write_text(text + "\n")
    print(text)


def command_summarize(args) -> None:
    out = Path(args.out).resolve()
    results: dict[str, list[dict]] = {}
    discarded: dict[str, int] = {}
    for path in sorted((out / "results").glob("*.json")):
        result = json.loads(path.read_text())
        if "window_drawn" not in result and kind_of(result["scenario"]) == "stream":
            # Runs recorded before the ratio existed: every drawn commit is a
            # frame, so fewer frames than 80% of commits means it was hidden.
            counters = result.get("counters", {})
            result["window_drawn"] = (
                counters.get("frames_per_s", 0) >= 0.8 * counters.get("commits_per_s", 0)
            )
        if result.get("window_drawn") is False:
            discarded[result["scenario"]] = discarded.get(result["scenario"], 0) + 1
            continue
        results.setdefault(result["scenario"], []).append(result)
    machine = read_json(out / "machine.json") or {}
    summary = {"schema": "waku.bench-baseline/1", "machine": machine, "scenarios": {}}
    for scenario in discarded:
        results.setdefault(scenario, [])
    for scenario, runs in results.items():
        fields = SUMMARY_FIELDS[kind_of(scenario)]
        summary["scenarios"][scenario] = {
            "runs": len(runs),
            "discarded_window_not_drawn": discarded.get(scenario, 0),
            "median": {field: median([get(run, field) for run in runs]) for field in fields},
            "range": {field: spread([get(run, field) for run in runs]) for field in fields},
            "per_run": [{field: get(run, field) for field in fields} for run in runs],
        }
    text = json.dumps(summary, indent=2)
    if args.raw:
        Path(args.raw).parent.mkdir(parents=True, exist_ok=True)
        Path(args.raw).write_text(text + "\n")
    if args.markdown:
        Path(args.markdown).write_text(markdown(summary))
    print(text)


def markdown(summary: dict) -> str:
    """Median tables, one per scenario kind, for pasting into a report."""
    lines = []
    by_kind: dict[str, list[str]] = {}
    for scenario in summary["scenarios"]:
        by_kind.setdefault(kind_of(scenario), []).append(scenario)
    for kind, scenarios in by_kind.items():
        fields = SUMMARY_FIELDS[kind]
        lines.append(f"### {kind}\n")
        lines.append("| metric | " + " | ".join(scenarios) + " |")
        lines.append("| --- |" + " ---: |" * len(scenarios))
        for field in fields:
            cells = []
            for scenario in scenarios:
                value = summary["scenarios"][scenario]["median"].get(field)
                bounds = summary["scenarios"][scenario].get("range", {}).get(field)
                cell = "" if value is None else f"{value:g}"
                if bounds and bounds[0] != bounds[1]:
                    cell += f" ({bounds[0]:g}–{bounds[1]:g})"
                cells.append(cell)
            lines.append(f"| `{field}` | " + " | ".join(cells) + " |")
        runs = ", ".join(
            f'{s}: {summary["scenarios"][s]["runs"]}'
            + (f' ({summary["scenarios"][s]["discarded_window_not_drawn"]} discarded)'
               if summary["scenarios"][s].get("discarded_window_not_drawn") else "")
            for s in scenarios)
        lines.append(f"\nMedian (min–max). Runs per scenario: {runs}.\n")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("--out", required=True)
    run.add_argument("--bins", default=str(REPO / "target" / "release"))
    run.add_argument("--rounds", type=int, default=3)
    run.add_argument("--scenarios", default="")
    run.add_argument("--first-round", type=int, default=1,
                     help="number of the first round (to add rounds to an --out)")
    measure = sub.add_parser("measure", help="sample any process tree (T3 Code)")
    measure.add_argument("--pid", required=True, help="root pid(s), comma-separated")
    measure.add_argument("--seconds", type=float, required=True)
    measure.add_argument("--label", default="")
    measure.add_argument("--json", default="")
    summarize = sub.add_parser("summarize")
    summarize.add_argument("--out", required=True)
    summarize.add_argument("--raw", default="")
    summarize.add_argument("--markdown", default="")
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, lambda *_: (stop_all(), sys.exit(143)))
    try:
        if args.command == "run":
            command_run(args)
        elif args.command == "measure":
            command_measure(args)
        else:
            command_summarize(args)
    finally:
        stop_all()
        shutil.rmtree(PROJECT_ROOT, ignore_errors=True)


if __name__ == "__main__":
    main()

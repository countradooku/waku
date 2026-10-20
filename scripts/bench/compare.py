#!/usr/bin/env python3
"""Decide whether head regressed against base from Criterion round data.

Each side directory holds `round-N/` Criterion homes written by compare.sh.
For every benchmark present on both sides this takes the Criterion median of
each round, then compares the median of those per-round medians:

    change = head / base - 1
    noise  = max over both sides of (max round - min round) / median round

A benchmark regresses when change > threshold AND change > noise: a slowdown
must clear both the agreed budget (10% by default) and the spread this very
machine showed between identical runs, so a noisy shared runner cannot fail a
pull request on jitter alone, while a real slowdown still does.
"""

import argparse
import json
import os
import pathlib
import statistics
import sys


def load(side: pathlib.Path) -> dict[str, list[float]]:
    medians: dict[str, list[float]] = {}
    for round_dir in sorted(side.glob("round-*")):
        for meta in sorted(round_dir.rglob("new/benchmark.json")):
            full_id = json.loads(meta.read_text())["full_id"]
            estimates = json.loads((meta.parent / "estimates.json").read_text())
            medians.setdefault(full_id, []).append(estimates["median"]["point_estimate"])
    return medians


def spread(values: list[float]) -> float:
    return (max(values) - min(values)) / statistics.median(values)


def human(ns: float) -> str:
    for unit, scale in (("s", 1e9), ("ms", 1e6), ("µs", 1e3)):
        if ns >= scale:
            return f"{ns / scale:.3g} {unit}"
    return f"{ns:.3g} ns"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=pathlib.Path, required=True)
    parser.add_argument("--head", type=pathlib.Path, required=True)
    parser.add_argument("--threshold", type=float, default=0.10)
    parser.add_argument("--report", type=pathlib.Path)
    args = parser.parse_args()

    base, head = load(args.base), load(args.head)
    rows, regressions = [], []
    for name in sorted(base.keys() | head.keys()):
        if name not in base or name not in head:
            side = "head" if name in head else "base"
            rows.append(f"| `{name}` | | | | | only in {side} |")
            continue
        base_ns = statistics.median(base[name])
        head_ns = statistics.median(head[name])
        change = head_ns / base_ns - 1
        noise = max(spread(base[name]), spread(head[name]))
        if change > args.threshold and change > noise:
            verdict = "**regression**"
            regressions.append(name)
        elif change < -args.threshold and -change > noise:
            verdict = "improvement"
        elif abs(change) > args.threshold:
            verdict = "inconclusive (within noise)"
        else:
            verdict = "ok"
        rows.append(
            f"| `{name}` | {human(base_ns)} | {human(head_ns)} | {change:+.1%} "
            f"| ±{noise:.1%} | {verdict} |"
        )

    rounds = max((len(v) for v in [*base.values(), *head.values()]), default=0)
    lines = [
        "## Benchmark comparison",
        "",
        f"Median of {rounds} alternating same-runner rounds per side. A benchmark "
        f"fails when it is more than {args.threshold:.0%} slower **and** slower "
        "than the round-to-round noise.",
        "",
        "| Benchmark | Base | Head | Change | Noise | Verdict |",
        "| --- | ---: | ---: | ---: | ---: | --- |",
        *rows,
        "",
    ]
    if regressions:
        lines.append(f"**{len(regressions)} regression(s):** " + ", ".join(regressions))
    elif not rows:
        lines.append("No benchmark results found.")
    else:
        lines.append("No regressions.")
    report = "\n".join(lines) + "\n"

    print(report)
    if args.report:
        args.report.write_text(report, encoding="utf-8")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as handle:
            handle.write(report)
    if not rows:
        return 2
    return 1 if regressions else 0


if __name__ == "__main__":
    sys.exit(main())

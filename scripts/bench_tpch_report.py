#!/usr/bin/env python3
"""Render TPC-H cold-process SQL measurements without hiding failed queries."""

import argparse
import json
import math
from pathlib import Path


def checked_time(value, label):
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value < 0:
        raise ValueError(f"Invalid {label} duration")
    return float(value)


def render_query_row(row):
    status = row.get("status")
    if status == "ok":
        median = checked_time(row.get("median_ms"), "median")
        p95 = checked_time(row.get("p95_ms"), "p95")
        if p95 + 0.001 < median:
            raise ValueError(f"p95 smaller than median in query {row['id']}")
        cells = [f"{median:.3f}", f"{p95:.3f}"]
    elif status == "error":
        cells = ["-", "-"]
    else:
        raise ValueError(f"Unknown query status: {status}")
    return f"| {row['id']:02} | {status} | {cells[0]} | {cells[1]} |"


def append_latency_concentration(lines, ok, results):
    if len(ok) == 22 and len(results) == 22:
        total_ms = sum(checked_time(row["median_ms"], "median") for row in ok)
        if total_ms > 0:
            slowest = sorted(ok, key=lambda row: row["median_ms"], reverse=True)[:3]
            lines.extend(["", "## Latency concentration", ""])
            for row in slowest:
                share = checked_time(row["median_ms"], "median") / total_ms * 100
                lines.append(f"- Q{row['id']:02}: {share:.1f}% of summed query medians")


def render(report):
    results = report.get("results")
    if not isinstance(results, list) or not results:
        raise ValueError("TPC-H report has no query results")
    ids = [row.get("id") for row in results]
    if any(isinstance(i, bool) or not isinstance(i, int) or i not in range(1, 23) for i in ids) or len(set(ids)) != len(ids):
        raise ValueError("Invalid or duplicate query IDs")
    ok = [row for row in results if row.get("status") == "ok"]
    lines = [
        "# TPC-H execution report", "",
        f"**Coverage:** {len(ok)}/{len(results)} queries successful",
        f"**Complete 22-query run:** {'yes' if len(ok) == 22 and len(results) == 22 else 'no'}",
        f"**Metric:** {report.get('metric', 'cold process wall-clock latency')}", "",
        "| Query | Status | Median (ms) | p95 (ms) |",
        "| ---: | --- | ---: | ---: |",
    ]
    for row in sorted(results, key=lambda item: item["id"]):
        lines.append(render_query_row(row))
    append_latency_concentration(lines, ok, results)
    failures = [row for row in results if row.get("status") != "ok"]
    if failures:
        lines.extend(["", "## Failures", ""])
        for row in failures:
            error = str(row.get("error", "unknown error")).replace("\n", " ").replace("|", "/")
            lines.append(f"- Q{row['id']:02}: {error[:280]}")
    lines.extend(["", "Cold CLI wall time includes startup, input registration, and result serialization.",
                  "These are local measurements, not TPC-certified benchmark scores.", ""])
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        result = render(json.loads(args.report.read_text(encoding="utf-8")))
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(result, encoding="utf-8")
        else:
            print(result, end="")
    except (OSError, ValueError, TypeError) as error:
        parser.exit(1, f"invalid report: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

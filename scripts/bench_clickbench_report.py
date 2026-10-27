#!/usr/bin/env python3
"""Render ClickBench query coverage and latencies without hiding failures."""

import argparse
import json
import math
from pathlib import Path


def timing(value, label):
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value < 0:
        raise ValueError(f"Invalid {label} timing")
    return float(value)


def format_result(row):
    state = row.get("status")
    failure = None
    if state == "executed":
        median = timing(row.get("median_ms"), "median")
        p95 = timing(row.get("p95_ms"), "p95")
        if p95 + .001 < median:
            raise ValueError(f"Invalid p95 for query {row['id']}")
        a, b = f"{median:.3f}", f"{p95:.3f}"
    elif state in ("error", "nondeterministic"):
        a, b = "-", "-"
        message = str(row.get("error", state)).replace("\n", " ").replace("|", "/")
        failure = f"- Q{row['id']:02}: {message[:250]}"
    else:
        raise ValueError(f"Unknown query status: {state}")
    return f"| {row['id']:02} | {state} | {a} | {b} |", failure


def render(report):
    entries = report.get("results")
    if not isinstance(entries, list) or not entries:
        raise ValueError("No ClickBench query entries")
    ids = [row.get("id") for row in entries]
    if any(type(i) is not int or i not in range(1, 44) for i in ids) or len(set(ids)) != len(ids):
        raise ValueError("Invalid or duplicated query ID")
    executed = sum(row.get("status") == "executed" for row in entries)
    lines = [
        "# ClickBench local run", "",
        f"**Queries executed:** {executed}/{len(entries)}",
        f"**Full official 43-query suite:** {'yes' if len(entries) == 43 else 'no'}",
        "**Metric:** cold CLI wall time, including startup, registration, SQL and JSON output",
        "", "| Query | Status | Median (ms) | p95 (ms) |",
        "| ---: | --- | ---: | ---: |",
    ]
    failures = []
    for row in sorted(entries, key=lambda item: item["id"]):
        line, failure = format_result(row)
        lines.append(line)
        if failure is not None:
            failures.append(failure)
    if failures:
        lines.extend(["", "## Failures", "", *failures])
    lines.extend(["", "Results are execution timings, not proof of correctness or official ClickBench scores.", ""])
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        rendered = render(json.loads(args.report.read_text(encoding="utf-8")))
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(rendered, encoding="utf-8")
        else:
            print(rendered, end="")
    except (OSError, ValueError, TypeError) as error:
        parser.exit(1, f"ClickBench report failed: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

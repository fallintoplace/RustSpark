#!/usr/bin/env python3
"""Summarize complete TPC-H runs without mixing failures into a speed score."""

import argparse
import json
import math
from pathlib import Path


def checked_medians(entries):
    medians = []
    for row in entries:
        if row.get("status") != "ok":
            raise ValueError(f"Query {row['id']} did not execute successfully")
        value = row.get("median_ms")
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"Invalid median for query {row['id']}")
        medians.append(float(value))
    return medians


def complete_query_rows(report):
    """Require one valid result for every TPC-H query ID."""
    entries = report.get("results")
    if not isinstance(entries, list) or len(entries) != 22:
        raise ValueError("A summary requires exactly 22 TPC-H queries")
    if (any(not isinstance(row, dict) or type(row.get("id")) is not int
            for row in entries)
            or {row["id"] for row in entries} != set(range(1, 23))):
        raise ValueError("Missing or duplicate TPC-H query ID")
    return entries


def summarize(report):
    entries = complete_query_rows(report)
    medians = checked_medians(entries)
    return {
        "queries_executed": 22,
        "metric": "sum and geometric mean of per-query cold CLI median wall times, not TPC throughput",
        "sum_median_ms": round(math.fsum(medians), 3),
        "geometric_mean_ms": round(math.exp(math.fsum(math.log(value) for value in medians) / 22), 3),
        "slowest_query": max(entries, key=lambda row: row["median_ms"])["id"],
        "fastest_query": min(entries, key=lambda row: row["median_ms"])["id"],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args(argv)
    try:
        result = summarize(json.loads(args.report.read_text(encoding="utf-8")))
        data = json.dumps(result, indent=2) + "\n"
        if args.json_out:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(data, encoding="utf-8")
        else:
            print(data, end="")
    except (OSError, ValueError, TypeError) as error:
        parser.exit(1, f"TPC-H summary failed: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

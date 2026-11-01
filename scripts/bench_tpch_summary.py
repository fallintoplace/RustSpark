#!/usr/bin/env python3
"""Summarize complete TPC-H runs without mixing failures into a speed score."""

import argparse
import json
import math
from pathlib import Path


def finite_sample_median(samples):
    ordered = sorted(samples)
    middle = len(ordered) // 2
    return ordered[middle] if len(ordered) % 2 else (
        ordered[middle - 1] + (ordered[middle] - ordered[middle - 1]) / 2
    )


def valid_positive_duration(value):
    if type(value) not in (int, float) or value <= 0:
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def checked_medians(entries):
    medians = []
    for row in entries:
        if row.get("status") != "ok":
            raise ValueError(f"Query {row['id']} did not execute successfully")
        value = row.get("median_ms")
        if not valid_positive_duration(value):
            raise ValueError(f"Invalid median for query {row['id']}")
        if "samples_ms" in row:
            samples = row["samples_ms"]
            if (not isinstance(samples, list) or not samples
                    or any(type(sample) not in (int, float)
                           or not valid_positive_duration(sample)
                           for sample in samples)):
                raise ValueError(f"Invalid timing samples for query {row['id']}")
            if abs(finite_sample_median(samples) - value) > 0.002:
                raise ValueError(f"Reported median differs from samples for query {row['id']}")
        medians.append(float(value))
    return medians


def complete_query_rows(report):
    """Require one valid result for every TPC-H query ID."""
    if not isinstance(report, dict):
        raise ValueError("TPC-H summary report must be a JSON object")
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
    try:
        total = math.fsum(medians)
    except OverflowError as error:
        raise ValueError("TPC-H latency sum exceeds finite range") from error
    slowest = sorted(entries, key=lambda row: (-row["median_ms"], row["id"]))[:3]
    return {
        "queries_executed": 22,
        "metric": "sum and geometric mean of per-query cold CLI median wall times, not TPC throughput",
        "sum_median_ms": round(total, 3),
        "top_latency_queries": [row["id"] for row in slowest],
        "top_three_latency_percent": round(
            math.fsum(row["median_ms"] for row in slowest) / total * 100, 2,
        ),
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

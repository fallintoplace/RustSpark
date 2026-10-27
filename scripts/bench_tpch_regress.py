#!/usr/bin/env python3
"""Compare two successful TPC-H reports by per-query cold-process medians."""

import argparse
import json
import math
from pathlib import Path


def checked_median(value, number):
    if (isinstance(value, bool) or not isinstance(value, (float, int))
            or not math.isfinite(value) or value <= 0):
        raise ValueError(f"Invalid median for query {number}")
    return float(value)


def medians(report):
    results = report.get("results")
    if not isinstance(results, list) or not results:
        raise ValueError("TPC-H query results are missing")
    values = {}
    for row in results:
        number = row.get("id")
        if type(number) is not int or number not in range(1, 23) or number in values:
            raise ValueError("Missing, repeated or invalid query ID")
        if row.get("status") != "ok":
            raise ValueError(f"Query {number} has not succeeded")
        values[number] = checked_median(row.get("median_ms"), number)
    return values


def check_matching_fingerprints(baseline, candidate):
    first = {entry["id"]: entry for entry in baseline["results"]}
    second = {entry["id"]: entry for entry in candidate["results"]}
    for number in sorted(first.keys() & second.keys()):
        for key in ("sql_sha256", "result_sha256"):
            left = first[number].get(key)
            right = second[number].get(key)
            if left is not None and right is not None and left != right:
                raise ValueError(f"Query {number} {key} mismatch between benchmark runs")


def compare(baseline, candidate, max_regression=0.20):
    if not math.isfinite(max_regression) or max_regression < 0:
        raise ValueError("Regression threshold must be nonnegative")
    old, new = medians(baseline), medians(candidate)
    if old.keys() != new.keys():
        raise ValueError("TPC-H query coverage differs between reports")
    check_matching_fingerprints(baseline, candidate)
    comparisons = []
    for number in sorted(old):
        ratio = new[number] / old[number]
        comparisons.append({
            "id": number,
            "baseline_ms": old[number],
            "candidate_ms": new[number],
            "ratio": round(ratio, 4),
            "regression": ratio > 1 + max_regression,
        })
    full_suite = set(old) == set(range(1, 23))
    suite_speedup = None
    if full_suite:
        log_ratio = math.fsum(math.log(old[q]) - math.log(new[q]) for q in old) / 22
        suite_speedup = round(math.exp(log_ratio), 4)
    return {"threshold_fraction": max_regression,
            "regressions": [x["id"] for x in comparisons if x["regression"]],
            "full_22_query_suite": full_suite,
            "geometric_mean_baseline_over_candidate": suite_speedup,
            "results": comparisons}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--max-regression", type=float, default=0.20)
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args(argv)
    try:
        result = compare(
            json.loads(args.baseline.read_text(encoding="utf-8")),
            json.loads(args.candidate.read_text(encoding="utf-8")),
            args.max_regression,
        )
    except (OSError, ValueError, TypeError) as error:
        parser.exit(1, f"invalid comparison: {error}\n")
    rendered = json.dumps(result, indent=2) + "\n"
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 1 if result["regressions"] else 0


if __name__ == "__main__":
    raise SystemExit(main())

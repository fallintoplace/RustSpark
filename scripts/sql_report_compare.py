#!/usr/bin/env python3
"""Compare SQL query-run reports and flag latency regressions."""

import argparse
import json
import math
from pathlib import Path


def reject_duplicate_keys(pairs):
    obj = {}
    for key, value in pairs:
        if key in obj:
            raise ValueError(f"Duplicate JSON key: {key}")
        obj[key] = value
    return obj


def reject_nonfinite(value):
    raise ValueError(f"Non-finite JSON value: {value}")


def load_report(path: Path) -> dict:
    data = json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=reject_nonfinite,
    )
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected report object")
    return data


def index_results(report: dict, label: str) -> dict[int, dict]:
    results = report.get("results")
    if not isinstance(results, list) or not results:
        raise ValueError(f"{label}: missing query results")
    count = report.get("queries_attempted")
    if type(count) is not int or len(results) != count:
        raise ValueError(f"{label}: inconsistent query count")
    indexed = {}
    for entry in results:
        if not isinstance(entry, dict) or type(entry.get("id")) is not int or entry["id"] <= 0:
            raise ValueError(f"{label}: invalid query entry")
        if entry["id"] in indexed:
            raise ValueError(f"{label}: duplicate query ID {entry['id']}")
        indexed[entry["id"]] = entry
    return indexed


def is_duration(value) -> bool:
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def compare_reports(reference: dict, candidate: dict, max_slowdown: float | None = None) -> dict:
    if max_slowdown is not None and (not is_duration(max_slowdown) or max_slowdown < 1):
        raise ValueError("--max-slowdown must be finite and at least 1")
    workload = reference.get("query_file_sha256")
    if not isinstance(workload, str) or not workload or workload != candidate.get("query_file_sha256"):
        raise ValueError("Reports use different SQL workloads")
    left = index_results(reference, "Reference")
    right = index_results(candidate, "Candidate")
    errors = []
    slowdowns = []
    timings = []
    for number in sorted(set(left) | set(right)):
        before, after = left.get(number), right.get(number)
        if before is None or after is None:
            errors.append(f"Query {number}: missing in one report")
            continue
        if not isinstance(before.get("sql"), str) or before["sql"] != after.get("sql"):
            errors.append(f"Query {number}: SQL differs")
            continue
        if before.get("status") != "executed" or after.get("status") != "executed":
            errors.append(f"Query {number}: query did not execute in both reports")
            continue
        if not isinstance(before.get("result_sha256"), str) or (
            before["result_sha256"] != after.get("result_sha256")
            or before.get("result_rows") != after.get("result_rows")
        ):
            errors.append(f"Query {number}: result differs")
            continue
        baseline_ms, candidate_ms = before.get("median_ms"), after.get("median_ms")
        if not is_duration(baseline_ms) or not is_duration(candidate_ms):
            errors.append(f"Query {number}: invalid latency")
            continue
        ratio = candidate_ms / baseline_ms if baseline_ms > 0 else None
        timings.append({
            "id": number, "reference_ms": baseline_ms,
            "candidate_ms": candidate_ms, "slowdown": ratio,
        })
        if ratio is not None and max_slowdown is not None and ratio > max_slowdown:
            slowdowns.append(number)
    return {
        "queries_compared": len(timings),
        "errors": errors,
        "slowdown_query_ids": slowdowns,
        "timings": timings,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reference", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--max-slowdown", type=float)
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args(argv)
    try:
        comparison = compare_reports(
            load_report(args.reference), load_report(args.candidate), args.max_slowdown,
        )
        if args.json_out is not None:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(json.dumps(comparison, indent=2) + "\n", encoding="utf-8")
    except (OSError, UnicodeError, ValueError) as error:
        parser.error(str(error))
    print(
        f"{comparison['queries_compared']} queries compared, "
        f"{len(comparison['errors'])} errors, "
        f"{len(comparison['slowdown_query_ids'])} slowdowns"
    )
    return 1 if comparison["errors"] or comparison["slowdown_query_ids"] else 0


if __name__ == "__main__":
    raise SystemExit(main())

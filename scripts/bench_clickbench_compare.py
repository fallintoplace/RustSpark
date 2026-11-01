#!/usr/bin/env python3
"""Compare ClickBench latency and result hashes for identical query IDs."""

import argparse
import json
import math
from pathlib import Path


def checked_entry(item, seen):
    if not isinstance(item, dict):
        raise ValueError("ClickBench result entry must be an object")
    query = item.get("id")
    if type(query) is not int or query not in range(1, 44) or query in seen:
        raise ValueError("Invalid or duplicate query ID")
    if item.get("status") != "executed":
        raise ValueError(f"Query {query} did not execute successfully")
    digest = item.get("result_sha256")
    if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise ValueError(f"Missing or invalid result hash in query {query}")
    median = item.get("median_ms")
    if type(median) not in (float, int):
        raise ValueError(f"Invalid query {query} median")
    try:
        median = float(median)
    except OverflowError as error:
        raise ValueError(f"Invalid query {query} median") from error
    if not math.isfinite(median) or median <= 0:
        raise ValueError(f"Invalid query {query} median")
    return query, {"hash": digest, "median": median}


def entries(report):
    if not isinstance(report, dict):
        raise ValueError("ClickBench report must be an object")
    results = report.get("results")
    if not isinstance(results, list) or not results:
        raise ValueError("Missing ClickBench results")
    checked = {}
    for item in results:
        query, value = checked_entry(item, checked)
        checked[query] = value
    return checked


def compare(baseline, candidate, threshold=0.2):
    if type(threshold) not in (float, int):
        raise ValueError("Threshold must be finite and nonnegative")
    try:
        threshold = float(threshold)
    except OverflowError as error:
        raise ValueError("Threshold must be finite and nonnegative") from error
    if not math.isfinite(threshold) or threshold < 0:
        raise ValueError("Threshold must be finite and nonnegative")
    if not isinstance(baseline, dict) or not isinstance(candidate, dict):
        raise ValueError("ClickBench reports must be objects")
    old_hash, new_hash = baseline.get("query_file_sha256"), candidate.get("query_file_sha256")
    if old_hash is not None or new_hash is not None:
        if (not isinstance(old_hash, str) or len(old_hash) != 64
                or any(ch not in "0123456789abcdef" for ch in old_hash)
                or old_hash != new_hash):
            raise ValueError("ClickBench SQL corpus differs between reports")
    old, new = entries(baseline), entries(candidate)
    if old.keys() != new.keys():
        raise ValueError("Query IDs differ between reports")
    compared = []
    for query in sorted(old):
        ratio = new[query]["median"] / old[query]["median"]
        if not math.isfinite(ratio):
            raise ValueError(f"ClickBench query {query} latency ratio is not finite")
        if ratio <= 0:
            raise ValueError(f"ClickBench query {query} latency ratio underflowed to zero")
        rounded = round(ratio, 4)
        reported_ratio = rounded if rounded > 0 else float(f"{ratio:.4g}")
        compared.append({
            "id": query,
            "same_result": old[query]["hash"] == new[query]["hash"],
            "median_ratio": reported_ratio,
            "regression": ratio > 1 + threshold,
        })
    return {
        "queries": len(compared),
        "result_mismatches": [r["id"] for r in compared if not r["same_result"]],
        "slowdowns": [r["id"] for r in compared if r["regression"]],
        "threshold": threshold, "results": compared,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--threshold", type=float, default=0.2)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        report = compare(json.loads(args.baseline.read_text(encoding="utf-8")),
                         json.loads(args.candidate.read_text(encoding="utf-8")),
                         args.threshold)
        data = json.dumps(report, indent=2) + "\n"
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(data, encoding="utf-8")
        else:
            print(data, end="")
    except (OSError, ValueError, TypeError) as error:
        parser.exit(1, f"ClickBench compare failed: {error}\n")
    return 1 if report["result_mismatches"] or report["slowdowns"] else 0


if __name__ == "__main__":
    raise SystemExit(main())

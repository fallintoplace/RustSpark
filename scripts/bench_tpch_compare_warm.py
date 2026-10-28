#!/usr/bin/env python3
"""Compare full warm TPC-H runs after independent DuckDB correctness verification."""

import argparse
import json
import math
import re
from pathlib import Path


def checked_medians(report):
    if "warm" not in str(report.get("metric", "")).lower():
        raise ValueError("Only warm-session reports may be compared")
    results = report.get("results")
    if not isinstance(results, list) or len(results) != 22:
        raise ValueError("Comparison requires exactly 22 TPC-H query results")
    out = {}
    for row in results:
        if not isinstance(row, dict):
            raise ValueError("Invalid TPC-H row")
        number = row.get("id")
        value = row.get("median_ms")
        if (type(number) is not int or number not in range(1, 23) or number in out
                or row.get("status") != "ok" or type(value) not in (float, int)
                or not math.isfinite(value) or value <= 0):
            raise ValueError("Invalid or incomplete warm TPC-H timings")
        out[number] = value
    return out


def checked_sql_hashes(report):
    hashes = {}
    for row in report["results"]:
        digest = row.get("sql_sha256")
        if type(digest) is not str or re.fullmatch(r"[0-9a-fA-F]{64}", digest) is None:
            raise ValueError("Warm comparison requires valid per-query SQL SHA-256 hashes")
        hashes[row["id"]] = digest
    return hashes


def verified_ids(report):
    results = report.get("results")
    if not isinstance(results, list) or len(results) != 22:
        raise ValueError("Reference verification must cover all 22 queries")
    matched = []
    for row in results:
        if not isinstance(row, dict) or row.get("status") != "match":
            continue
        expected, actual = row.get("expected_rows"), row.get("actual_rows")
        if (type(expected) is not int or expected < 0
                or type(actual) is not int or actual != expected):
            raise ValueError("Reference verification has invalid row counts")
        matched.append(row.get("id"))
    if len(matched) != 22 or set(matched) != set(range(1, 23)) or len(set(matched)) != 22:
        raise ValueError("TPC-H row-value reference verification is incomplete")
    return set(matched)


def compare(rustspark, duckdb, verified):
    verified_ids(verified)
    rs, dd = checked_medians(rustspark), checked_medians(duckdb)
    if rs.keys() != dd.keys():
        raise ValueError("TPC-H query coverage differs")
    verified_hashes = checked_sql_hashes(rustspark)
    if verified_hashes != checked_sql_hashes(duckdb):
        raise ValueError("Warm TPC-H reports use different SQL query files")
    reference_hashes = {row["id"]: row.get("sql_sha256")
                        for row in verified["results"]}
    if verified_hashes != reference_hashes:
        raise ValueError("Reference verification uses different SQL query files")
    rows = []
    for number in sorted(rs):
        ratio = dd[number] / rs[number]
        rows.append({"id": number, "rustspark_median_ms": rs[number],
                     "duckdb_median_ms": dd[number], "duckdb_over_rustspark": round(ratio, 4),
                     "rustspark_faster": ratio > 1})
    geo = math.exp(math.fsum(math.log(dd[n]) - math.log(rs[n]) for n in rs) / 22)
    return {
        "metric": "per-query warm-session latency ratio; DuckDB / RustSpark",
        "warning": "Cross-engine serialization differs; not official TPC-H throughput",
        "verified_queries": 22,
        "rustspark_faster_queries": sum(row["rustspark_faster"] for row in rows),
        "geometric_mean_duckdb_over_rustspark": round(geo, 4),
        "rustspark_partitions": rustspark.get("target_partitions"),
        "duckdb_threads": duckdb.get("threads"),
        "queries": rows,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("rustspark", type=Path, help="bench_tpch_warm JSON report")
    parser.add_argument("duckdb", type=Path, help="bench_tpch_duckdb_warm JSON report")
    parser.add_argument("--verification", type=Path, required=True,
                        help="bench_tpch_verify full 22-query output matching report")
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args(argv)
    try:
        data = compare(json.loads(args.rustspark.read_text(encoding="utf-8")),
                       json.loads(args.duckdb.read_text(encoding="utf-8")),
                       json.loads(args.verification.read_text(encoding="utf-8")))
        output = json.dumps(data, indent=2) + "\n"
        if args.json_out:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(output, encoding="utf-8")
        print(output, end="")
    except (OSError, TypeError, ValueError, OverflowError) as error:
        parser.exit(1, f"Invalid TPC-H warm comparison: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Evaluate TPC-H end-to-end latency across target-partition settings."""

import argparse
import json
import math
import re
from pathlib import Path
import subprocess
import sys


def parse_partitions(value):
    result = []
    for item in value.split(","):
        if not item.strip().isdigit() or int(item) < 1:
            raise ValueError("Partitions must be comma-separated positive integers")
        number = int(item)
        if number in result:
            raise ValueError("Duplicate partition count")
        result.append(number)
    if not result:
        raise ValueError("No partition counts supplied")
    return result


def run_command(binary, queries, parquet, partitions, json_out, repeat, warmup):
    return [sys.executable, str(Path(__file__).with_name("tpch_run.py")),
            "--binary", binary, "--queries-dir", str(queries), "--parquet-dir", str(parquet),
            "--target-partitions", str(partitions), "--repeat", str(repeat),
            "--warmup", str(warmup), "--json-out", str(json_out)]


def verify_query_fingerprints(results, fingerprints):
    for row in results:
        if row.get("status") != "ok":
            continue
        number = row.get("id")
        digest = row.get("result_sha256")
        if type(number) is not int or number not in range(1, 23):
            raise ValueError("Invalid TPC-H query ID in partition report")
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise ValueError(f"Missing result fingerprint for Q{number:02}")
        if number in fingerprints and fingerprints[number] != digest:
            raise ValueError(f"Query Q{number:02} changed answers across partitions")
        fingerprints[number] = digest


def run_configuration(binary, queries, parquet, output, n, repeat, warmup, fingerprints):
    path = output / f"partitions-{n}.json"
    if path.exists():
        raise ValueError(f"Report already exists: {path}")
    process = subprocess.run(run_command(binary, queries, parquet, n, path, repeat, warmup),
                             capture_output=True, text=True, check=False)
    if not path.is_file():
        raise RuntimeError(f"No benchmark report for {n} partitions: {process.stderr[-400:]}")
    report = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(report, dict):
        raise ValueError("Partition matrix report must be a JSON object")
    results = report.get("results")
    if not isinstance(results, list) or any(not isinstance(row, dict) for row in results):
        raise ValueError("Partition matrix results must be JSON objects")
    ids = [row.get("id") for row in results]
    if (any(type(number) is not int or number not in range(1, 23) for number in ids)
            or len(set(ids)) != len(ids)):
        raise ValueError("Invalid or duplicate TPC-H query IDs in partition report")
    ok = sum(row.get("status") == "ok" for row in results)
    verify_query_fingerprints(results, fingerprints)
    complete = (len(results) == 22 and ok == 22 and process.returncode == 0
                and {row.get("id") for row in results} == set(range(1, 23)))
    total = None
    if complete:
        times = [row.get("median_ms") for row in results]
        if any(type(value) not in (int, float) or not math.isfinite(value) or value <= 0
               for value in times):
            raise ValueError(f"Invalid median in {n}-partition full-suite report")
        total = round(math.fsum(times), 3)
    return {"partitions": n, "queries_attempted": len(results),
                           "queries_ok": ok, "runner_exit": process.returncode,
                           "sum_median_ms": total, "report": path.name}


def matrix(binary, queries, parquet, output, partitions, repeat=3, warmup=1):
    if repeat < 1 or warmup < 0:
        raise ValueError("Repeat and warmup must be valid")
    output.mkdir(parents=True, exist_ok=True)
    configurations = []
    fingerprints = {}
    for n in partitions:
        configurations.append(
            run_configuration(binary, queries, parquet, output, n, repeat, warmup, fingerprints)
        )
    complete_runs = [row for row in configurations if row["sum_median_ms"] is not None]
    best = min(complete_runs, key=lambda row: row["sum_median_ms"]) if complete_runs else None
    summary = {"metric": "cold CLI + Parquet registration + SQL + JSON output",
               "best_complete_partitions": best["partitions"] if best else None,
               "configurations": configurations}
    (output / "matrix.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--queries-dir", required=True, type=Path)
    parser.add_argument("--parquet-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--partitions", default="1,2,4,8")
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    args = parser.parse_args(argv)
    try:
        parts = parse_partitions(args.partitions)
        summary = matrix(args.binary, args.queries_dir, args.parquet_dir,
                         args.output_dir, parts, args.repeat, args.warmup)
    except (OSError, ValueError, RuntimeError) as error:
        parser.exit(1, f"matrix failed: {error}\n")
    print(json.dumps(summary, indent=2))
    return 0 if all(c["queries_ok"] == 22 and c["queries_attempted"] == 22
                    and c["runner_exit"] == 0 for c in summary["configurations"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())

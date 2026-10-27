#!/usr/bin/env python3
"""Capture logical/physical EXPLAIN plans for numbered TPC-H SQL files."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess

from tpch_run import input_arguments, query_paths


def command(binary, table_arguments, sql_file, partitions):
    if partitions < 1:
        raise ValueError("Target partitions must be positive")
    return [str(binary), *table_arguments, "--sql-file", str(sql_file),
            "--target-partitions", str(partitions), "--explain"]


def run_explain(binary, tables, sql_file, partitions, timeout):
    process = subprocess.run(
        command(binary, tables, sql_file, partitions),
        capture_output=True, text=True, timeout=timeout, check=False,
    )
    if process.returncode:
        raise RuntimeError(f"Exit {process.returncode}: {process.stderr[-500:]}")
    if not process.stdout.strip():
        raise ValueError("Empty EXPLAIN result")
    return process.stdout


def capture(binary, parquet, queries, output, timeout=120, partitions=2):
    if timeout < 1 or output.exists():
        raise ValueError("Timeout must be positive; output directory must not exist")
    tables = input_arguments(parquet)
    output.mkdir(parents=True)
    results = []
    for number, file in queries:
        destination = output / f"{number:02}.txt"
        entry = {"id": number, "plan_file": destination.name}
        try:
            plan = run_explain(binary, tables, file, partitions, timeout)
            destination.write_text(plan, encoding="utf-8")
            plan_bytes = plan.encode("utf-8")
            entry["plan_sha256"] = hashlib.sha256(plan_bytes).hexdigest()
            entry["plan_bytes"] = len(plan_bytes)
            entry["status"] = "ok"
        except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
            entry.update({"status": "error", "error": str(error)[-500:]})
        results.append(entry)
    (output / "index.json").write_text(json.dumps({"results": results}, indent=2) + "\n")
    return results


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--parquet-dir", required=True, type=Path)
    parser.add_argument("--queries-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--query-ids", type=int, nargs="+")
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--target-partitions", type=int, default=2)
    args = parser.parse_args(argv)
    try:
        results = capture(args.binary, args.parquet_dir,
                          query_paths(args.queries_dir, args.query_ids),
                          args.output_dir, args.timeout, args.target_partitions)
    except (OSError, ValueError) as error:
        parser.exit(1, f"TPC-H EXPLAIN failed: {error}\n")
    passed = sum(item["status"] == "ok" for item in results)
    print(f"EXPLAIN: {passed}/{len(results)} succeeded")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())

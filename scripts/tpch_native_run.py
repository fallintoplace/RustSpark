#!/usr/bin/env python3
"""Attempt the 22 TPC-H SQL files using the current native RustSpark CLI.

Execution success is not a DuckDB result-parity claim.
"""
import argparse
import json
from pathlib import Path
import subprocess

from tpch_run import input_arguments, query_paths, result_signature
from clickbench_run import has_top_level_order_by


def run_suite(binary, queries, data, timeout):
    if isinstance(timeout, bool) or timeout <= 0:
        raise ValueError("Timeout must be positive")
    sources = input_arguments(data)
    results = []
    for number, sql_file in queries:
        command = [
            str(binary), *sources, "--sql-file", str(sql_file),
            "--stdout-format", "json",
        ]
        try:
            sql = sql_file.read_text(encoding="utf-8")
            process = subprocess.run(command, check=False, capture_output=True,
                                     text=True, timeout=timeout)
            if process.returncode:
                raise RuntimeError(
                    f"native SQL exited {process.returncode}: {process.stderr[-600:]}"
                )
            digest, row_count = result_signature(
                process.stdout, ordered=has_top_level_order_by(sql)
            )
            results.append({
                "id": number, "status": "executed", "result_rows": row_count,
                "result_sha256": digest,
            })
        except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
            results.append({"id": number, "status": "error", "error": str(exc)[-600:]})
    return {
        "engine": "rustspark-native",
        "validation": "SQL execution only, not DuckDB value parity",
        "results": results,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--queries-dir", type=Path, required=True)
    parser.add_argument("--parquet-dir", type=Path, required=True)
    parser.add_argument("--query-ids", type=int, nargs="+")
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--require-all", action="store_true")
    args = parser.parse_args(argv)
    try:
        queries = query_paths(args.queries_dir, args.query_ids)
        report = run_suite(args.binary, queries, args.parquet_dir, args.timeout)
    except (OSError, ValueError) as exc:
        parser.exit(1, f"error: {exc}\n")
    rendered = json.dumps(report, indent=2, allow_nan=False)
    if args.json_out:
        args.json_out.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    failed = [entry["id"] for entry in report["results"]
              if entry["status"] != "executed"]
    if args.require_all and (failed or {entry["id"] for entry in report["results"]} != set(range(1, 23))):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Evaluate TPC-H end-to-end latency across target-partition settings."""

import argparse
import json
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


def run_configuration(binary, queries, parquet, output, partitions, repeat, warmup):
    path = output / f"partitions-{partitions}.json"
    if path.exists():
        raise ValueError(f"Report already exists: {path}")
    process = subprocess.run(
        run_command(binary, queries, parquet, partitions, path, repeat, warmup),
        capture_output=True, text=True, check=False,
    )
    if not path.is_file():
        raise RuntimeError(
            f"No benchmark report for {partitions} partitions: {process.stderr[-400:]}"
        )
    report = json.loads(path.read_text(encoding="utf-8"))
    results = report.get("results", [])
    ok = sum(row.get("status") == "ok" for row in results)
    return {"partitions": partitions, "queries_attempted": len(results),
            "queries_ok": ok, "runner_exit": process.returncode,
            "report": path.name}


def matrix(binary, queries, parquet, output, partitions, repeat=3, warmup=1):
    if repeat < 1 or warmup < 0:
        raise ValueError("Repeat and warmup must be valid")
    output.mkdir(parents=True, exist_ok=True)
    configurations = []
    for n in partitions:
        configurations.append(
            run_configuration(binary, queries, parquet, output, n, repeat, warmup)
        )
    summary = {"metric": "cold CLI + Parquet registration + SQL + JSON output",
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

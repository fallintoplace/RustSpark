#!/usr/bin/env python3
"""Run numbered SQL files against local Parquet tables in fresh processes."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import platform
import statistics
import subprocess
import time

from clickbench_run import has_top_level_order_by

TABLES = ("customer", "lineitem", "nation", "orders",
          "part", "partsupp", "region", "supplier")


def reject_nonfinite(value):
    raise ValueError(f"Invalid JSON number: {value}")


def unique_keys(pairs):
    output = {}
    for name, value in pairs:
        if name in output:
            raise ValueError(f"Duplicate result key: {name}")
        output[name] = value
    return output


def result_signature(stdout, ordered=False):
    rows = []
    for line in stdout.splitlines():
        if not line.strip():
            continue
        value = json.loads(
            line, parse_constant=reject_nonfinite, object_pairs_hook=unique_keys
        )
        if not isinstance(value, dict):
            raise ValueError("Expected a JSON object for each SQL result row")
        rows.append(json.dumps(value, sort_keys=True, allow_nan=False))
    if not ordered:
        rows.sort()
    normalized = "\n".join(rows)
    return hashlib.sha256(normalized.encode()).hexdigest(), len(rows)


def query_paths(directory, ids=None):
    paths = sorted(directory.glob("[0-9][0-9].sql"))
    if not paths:
        raise ValueError(f"No numbered SQL files found in {directory}")
    selected = [(int(path.stem), path) for path in paths]
    if ids is not None:
        selected = [(n, path) for n, path in selected if n in ids]
        if {n for n, _ in selected} != set(ids):
            raise ValueError("A requested SQL query file is missing")
    return selected


def input_arguments(directory):
    args = []
    for name in TABLES:
        path = directory / f"{name}.parquet"
        if not path.is_file():
            raise ValueError(f"Missing Parquet table: {path}")
        args.extend(["--parquet-table", f"{name}={path.resolve()}"])
    return args


def measure(command, repeat, warmup, timeout, ordered=False):
    samples = []
    digest = None
    count = 0
    for attempt in range(repeat + warmup):
        started = time.perf_counter()
        process = subprocess.run(
            command, text=True, capture_output=True, check=False, timeout=timeout
        )
        elapsed = (time.perf_counter() - started) * 1000
        if process.returncode:
            raise RuntimeError(
                f"SQL exited {process.returncode}: {process.stderr.strip()[-600:]}"
            )
        current, count = result_signature(process.stdout, ordered=ordered)
        if digest is not None and current != digest:
            raise ValueError("Nondeterministic SQL results across repetitions")
        digest = current
        if attempt >= warmup:
            samples.append(round(elapsed, 3))
    return {
        "status": "ok",
        "result_sha256": digest,
        "result_rows": count,
        "samples_ms": samples,
        "median_ms": round(statistics.median(samples), 3),
        "p95_ms": sorted(samples)[math.ceil(0.95 * len(samples)) - 1],
    }


def command_for(binary, input_args, sql_file, partitions):
    return [
        str(binary), *input_args, "--sql-file", str(sql_file),
        "--stdout-format", "json", "--target-partitions", str(partitions),
    ]


def benchmark(binary, queries, parquet, repeat=3, warmup=1, timeout=120, partitions=2):
    if repeat < 1 or warmup < 0 or timeout < 1 or partitions < 1:
        raise ValueError("Invalid repeat, warmup, timeout or partitions")
    args = input_arguments(parquet)
    reports = []
    for number, path in queries:
        command = command_for(binary, args, path, partitions)
        entry = {"id": number, "sql_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        try:
            sql = path.read_text(encoding="utf-8")
            entry.update(measure(
                command, repeat, warmup, timeout,
                ordered=has_top_level_order_by(sql),
            ))
        except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
            entry.update({"status": "error", "error": str(error)[-900:]})
        reports.append(entry)
    return {
        "metric": "cold process + Parquet registration + SQL + JSON output",
        "warning": "Local end-to-end latency, not warm or distributed throughput",
        "platform": platform.platform(),
        "repeat": repeat,
        "warmup": warmup,
        "results": reports,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--queries-dir", type=Path, required=True)
    parser.add_argument("--parquet-dir", type=Path, required=True)
    parser.add_argument("--query-ids", nargs="+", type=int)
    parser.add_argument("--require-all", action="store_true",
                        help="Fail unless all 22 numbered TPC-H queries run successfully")
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--target-partitions", type=int, default=2)
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args(argv)
    try:
        report = benchmark(
            args.binary, query_paths(args.queries_dir, args.query_ids),
            args.parquet_dir, args.repeat, args.warmup,
            args.timeout, args.target_partitions,
        )
    except (OSError, ValueError) as error:
        parser.exit(1, f"error: {error}\n")
    rendered = json.dumps(report, indent=2) + "\n"
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    successful = {row["id"] for row in report["results"] if row["status"] == "ok"}
    attempted = [row["id"] for row in report["results"]]
    if len(successful) != len(attempted):
        return 1
    if args.require_all and successful != set(range(1, 23)):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

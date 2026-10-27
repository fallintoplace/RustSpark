#!/usr/bin/env python3
"""Measure warm DuckDB SQL on the same Parquet tables and query files as RustSpark."""

import argparse
import json
import math
from pathlib import Path
import statistics
import time

from clickbench_run import has_top_level_order_by
from tpch_run import query_paths, result_signature
from bench_tpch_verify import reference_connection


def measure_query(connection, sql, warmup, repeat):
    if warmup < 0 or repeat < 1:
        raise ValueError("Invalid warmup or repeat")
    samples = []
    fingerprint = None
    row_count = None
    ordered = has_top_level_order_by(sql)
    for attempt in range(warmup + repeat):
        started = time.perf_counter()
        cursor = connection.execute(sql)
        columns = [field[0] for field in cursor.description]
        if len(columns) != len(set(columns)):
            raise ValueError("Duplicate SQL column names")
        rows = [dict(zip(columns, values)) for values in cursor.fetchall()]
        encoded = "\n".join(json.dumps(row, default=str, allow_nan=False) for row in rows)
        elapsed = (time.perf_counter() - started) * 1000
        signature, count = result_signature(encoded, ordered=ordered)
        if fingerprint is not None and fingerprint != signature:
            raise ValueError("Nondeterministic DuckDB query results")
        if row_count is not None and row_count != count:
            raise ValueError("DuckDB query row count changed")
        fingerprint, row_count = signature, count
        if not math.isfinite(elapsed) or elapsed < 0:
            raise ValueError("Invalid warm query timing")
        if attempt >= warmup:
            samples.append(elapsed)
    return {"status": "ok", "result_sha256": fingerprint, "result_rows": row_count,
            "samples_ms": [round(x, 3) for x in samples],
            "median_ms": round(statistics.median(samples), 3),
            "p95_ms": round(sorted(samples)[math.ceil(0.95 * len(samples)) - 1], 3)}


def benchmark(parquet, queries, warmup=1, repeat=5, threads=2):
    if type(threads) is not int or threads < 1:
        raise ValueError("Threads must be a positive integer")
    connection = reference_connection(parquet)
    results = []
    try:
        connection.execute(f"SET threads={threads}")
        for number, path in queries:
            sql = path.read_text(encoding="utf-8")
            item = {"id": number, "sql_sha256": __import__("hashlib").sha256(path.read_bytes()).hexdigest()}
            try:
                item.update(measure_query(connection, sql, warmup, repeat))
            except (ValueError, RuntimeError) as error:
                item.update({"status": "error", "error": str(error)[-500:]})
            results.append(item)
    finally:
        connection.close()
    return {"metric": "warm DuckDB SQL planning, execution and Python JSON serialization",
            "warning": "Different serializer from RustSpark; not TPC-certified throughput",
            "warmup": warmup, "repeat": repeat, "threads": threads, "results": results}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet-dir", type=Path, required=True)
    parser.add_argument("--queries-dir", type=Path, required=True)
    parser.add_argument("--query-ids", type=int, nargs="+")
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args(argv)
    try:
        report = benchmark(args.parquet_dir, query_paths(args.queries_dir, args.query_ids),
                           args.warmup, args.repeat, args.threads)
        rendered = json.dumps(report, indent=2) + "\n"
        if args.json_out:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(rendered, encoding="utf-8")
        print(rendered, end="")
    except (OSError, ImportError, ValueError, RuntimeError) as error:
        parser.exit(1, f"Warm DuckDB benchmark failed: {error}\n")
    return 0 if report["results"] and all(row["status"] == "ok" for row in report["results"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())

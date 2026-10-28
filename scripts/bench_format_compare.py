#!/usr/bin/env python3
"""Compare identical deterministic SQL results over CSV and Parquet.

DuckDB is only used to convert the generated CSV to Parquet. The same RustSpark
binary and SQL workload execute both formats; process startup is included.
"""

import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import time

from bench_rustspark import WORKLOADS, expected_results, output_signature, write_csv
from bench_stats import summarize_samples


def convert_to_parquet(csv, parquet):
    import duckdb
    source = str(csv.resolve()).replace("'", "''")
    destination = str(parquet.resolve()).replace("'", "''")
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            f"COPY (SELECT CAST(id AS BIGINT) AS id, CAST(city AS VARCHAR) AS city, "
            f"CAST(amount AS BIGINT) AS amount FROM read_csv_auto('{source}', header=true)) "
            f"TO '{destination}' (FORMAT PARQUET)"
        )
    finally:
        connection.close()


def command(binary, format_name, file, sql):
    if format_name == "csv":
        inputs = ["--csv", str(file), "--csv-schema", "id:int64,city:utf8,amount:int64"]
    elif format_name == "parquet":
        inputs = ["--parquet", str(file)]
    else:
        raise ValueError("Unknown input format")
    return [binary, *inputs, "--sql", sql, "--stdout-format", "json"]


def verified_elapsed(binary, kind, file, sql, expected_hash, timeout):
    started = time.perf_counter()
    output = subprocess.run(
        command(binary, kind, file, sql), capture_output=True,
        text=True, timeout=timeout, check=True,
    )
    elapsed = (time.perf_counter() - started) * 1000
    digest, _ = output_signature(output.stdout)
    if digest != expected_hash:
        raise ValueError(f"Wrong SQL result for {kind} input")
    return elapsed


def measure(binary, kind, file, sql, expected_hash, repeat, warmup, timeout):
    values = []
    for index in range(repeat + warmup):
        elapsed = verified_elapsed(binary, kind, file, sql, expected_hash, timeout)
        if index >= warmup:
            values.append(elapsed)
    return {**summarize_samples(values), "result_sha256": expected_hash}


def benchmark(binary, rows=100000, repeat=3, warmup=1, timeout=120):
    if min(rows, repeat, timeout) < 1 or warmup < 0:
        raise ValueError("rows/repeat/timeout must be positive; warmup must be nonnegative")
    with tempfile.TemporaryDirectory(prefix="rustspark-formats-") as folder:
        root = Path(folder)
        csv, parquet = root / "input.csv", root / "input.parquet"
        write_csv(csv, rows)
        convert_to_parquet(csv, parquet)
        csv_bytes, parquet_bytes = csv.stat().st_size, parquet.stat().st_size
        expected = expected_results(rows)
        output = []
        for name, sql in WORKLOADS.items():
            digest = output_signature("\n".join(json.dumps(row) for row in expected[name]))[0]
            output.append({
                "workload": name, "sql": sql,
                "csv": measure(binary, "csv", csv, sql, digest, repeat, warmup, timeout),
                "parquet": measure(binary, "parquet", parquet, sql, digest, repeat, warmup, timeout),
            })
        return {"metric": "cold process + file registration + SQL + JSON output",
                "rows": rows, "csv_bytes": csv_bytes, "parquet_bytes": parquet_bytes,
                "repeat": repeat, "warmup": warmup, "workloads": output}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--json-out", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        result = benchmark(args.binary, args.rows, args.repeat, args.warmup, args.timeout)
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    except (OSError, ValueError, ImportError, RuntimeError, subprocess.SubprocessError) as error:
        parser.exit(1, f"cross-format benchmark failed: {error}\n")
    print(f"Verified {len(result['workloads'])} workloads on CSV and Parquet")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

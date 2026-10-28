#!/usr/bin/env python3
"""Deterministic small star-schema joins for RustSpark SQL microbenchmarking.

This is synthetic star-schema SQL, not the official Star Schema Benchmark.
Times include process startup, CSV registration, query and JSON serialization.
"""

import argparse
import json
import math
from pathlib import Path
import subprocess
import tempfile
import time

from bench_rustspark import output_signature
from bench_stats import summarize_samples


WORKLOADS = {
    "fact_product_count": (
        "SELECT COUNT(*) AS n FROM facts f "
        "JOIN products p ON f.dim = p.id"
    ),
    "three_table_sum": (
        "SELECT SUM(f.amount) AS total FROM facts f "
        "JOIN products p ON f.dim = p.id "
        "JOIN regions r ON p.dim = r.id"
    ),
    "region_revenue": (
        "SELECT r.id AS region, SUM(f.amount) AS total FROM facts f "
        "JOIN products p ON f.dim = p.id "
        "JOIN regions r ON p.dim = r.id "
        "GROUP BY r.id ORDER BY r.id"
    ),
    "filtered_join": (
        "SELECT COUNT(*) AS n FROM facts f "
        "JOIN products p ON f.dim = p.id "
        "JOIN regions r ON p.dim = r.id "
        "WHERE f.amount >= 50"
    ),
}


def validate(rows, products, regions):
    if min(rows, products, regions) < 1:
        raise ValueError("rows, products, and regions must be positive")


def _write_csv_table(path, records):
    with path.open("w", encoding="utf-8") as stream:
        stream.write("id,dim,amount\n")
        for identifier, dimension, amount in records:
            stream.write(f"{identifier},{dimension},{amount}\n")


def fixtures(root, rows, products, regions):
    validate(rows, products, regions)
    inputs = {
        "facts": root / "facts.csv",
        "products": root / "products.csv",
        "regions": root / "regions.csv",
    }
    _write_csv_table(inputs["facts"], (
        (index, index % products, index * 17 % 100) for index in range(rows)
    ))
    _write_csv_table(inputs["products"], (
        (index, index % regions, 0) for index in range(products)
    ))
    _write_csv_table(inputs["regions"], (
        (index, 0, 0) for index in range(regions)
    ))
    return inputs


def answers(rows, products, regions):
    validate(rows, products, regions)
    revenue = {}
    filtered = 0
    total = 0
    # Product keys and amounts repeat after their least common multiple.
    period = math.lcm(products, 100)
    cycles, remainder = divmod(rows, period)
    for index in range(min(rows, period)):
        occurrences = cycles + (index < remainder)
        region = (index % products) % regions
        amount = (index * 17) % 100
        revenue[region] = revenue.get(region, 0) + amount * occurrences
        total += amount * occurrences
        filtered += (amount >= 50) * occurrences
    return {
        "fact_product_count": [{"n": rows}],
        "three_table_sum": [{"total": total}],
        "region_revenue": [{"region": region, "total": value}
                           for region, value in sorted(revenue.items())],
        "filtered_join": [{"n": filtered}],
    }


def command(binary, inputs, sql):
    arguments = [str(binary)]
    for name in ("facts", "products", "regions"):
        arguments.extend(["--csv-table", f"{name}={inputs[name]}"])
    return [*arguments, "--csv-schema", "id:int64,dim:int64,amount:int64",
            "--sql", sql, "--stdout-format", "json"]


def measure(binary, inputs, sql, expected_hash, repeat, warmup, timeout):
    durations = []
    for attempt in range(repeat + warmup):
        started = time.perf_counter()
        output = subprocess.run(command(binary, inputs, sql),
                                capture_output=True, text=True, check=True, timeout=timeout)
        elapsed = (time.perf_counter() - started) * 1000
        actual_hash, _ = output_signature(output.stdout)
        if actual_hash != expected_hash:
            raise ValueError("Incorrect star-schema SQL result")
        if attempt >= warmup:
            durations.append(elapsed)
    return {**summarize_samples(durations), "result_sha256": expected_hash}


def benchmark(binary, rows=100000, products=100, regions=10, repeat=3, warmup=1, timeout=120):
    validate(rows, products, regions)
    if repeat < 1 or warmup < 0 or timeout < 1:
        raise ValueError("Invalid timing options")
    with tempfile.TemporaryDirectory(prefix="rustspark-star-") as temp:
        files = fixtures(Path(temp), rows, products, regions)
        expected = answers(rows, products, regions)
        workloads = []
        for name, sql in WORKLOADS.items():
            digest = output_signature("\n".join(json.dumps(item) for item in expected[name]))[0]
            workloads.append({"name": name, "sql": sql,
                              **measure(binary, files, sql, digest, repeat, warmup, timeout)})
        return {
            "description": "Synthetic local star joins, not official Star Schema Benchmark",
            "metric": "cold-process wall time with CSV registration and JSON output",
            "rows": rows, "products": products, "regions": regions,
            "repeat": repeat, "warmup": warmup,
            "workloads": workloads,
        }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--products", type=int, default=100)
    parser.add_argument("--regions", type=int, default=10)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--json-out", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = benchmark(args.binary, args.rows, args.products, args.regions,
                           args.repeat, args.warmup, args.timeout)
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        parser.exit(1, f"star-schema benchmark failed: {error}\n")
    print(f"Verified {len(result['workloads'])} star-schema join workloads")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

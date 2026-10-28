#!/usr/bin/env python3
"""Qualify a reproducible billion-row TPC-H run using its recorded artifacts.

This validates report consistency, not a particular latency target or speedup.
Use --check-files to independently hash the actual Parquet fixture.
"""

import argparse
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
from pathlib import Path
import statistics

TABLES = ("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")
FULL_SUITE = set(range(1, 23))


def checked_hash(value, label):
    if (not isinstance(value, str) or len(value) != 64
            or any(char not in "0123456789abcdef" for char in value)):
        raise ValueError(f"{label} must be a lowercase SHA-256 digest")
    return value


def checked_number(value, label):
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{label} must be a positive finite number")
    return value


def checked_entries(document, label):
    entries = document.get("results")
    if (not isinstance(entries, list) or len(entries) != 22
            or any(not isinstance(row, dict) or type(row.get("id")) is not int
                   for row in entries)
            or {row["id"] for row in entries} != FULL_SUITE):
        raise ValueError(f"{label} must contain exactly 22 unique TPC-H queries")
    return {row["id"]: row for row in entries}


def fixture_queries(manifest):
    queries = manifest.get("queries")
    if (not isinstance(queries, list) or len(queries) != 22
            or any(not isinstance(row, dict) or type(row.get("id")) is not int
                   for row in queries)
            or {row["id"] for row in queries} != FULL_SUITE):
        raise ValueError("Fixture manifest requires all 22 unique SQL query IDs")
    return {row["id"]: checked_hash(row.get("sha256"), f"query {row['id']}")
            for row in queries}


def check_fixture(manifest, fixture_dir=None):
    rows = manifest.get("rows")
    hashes = manifest.get("table_sha256")
    paths = manifest.get("tables")
    if not all(isinstance(value, dict) for value in (rows, hashes, paths)):
        raise ValueError("Fixture manifest is missing table metadata")
    if set(rows) != set(TABLES) or set(hashes) != set(TABLES) or set(paths) != set(TABLES):
        raise ValueError("Fixture manifest must identify all eight TPC-H tables")
    for table in TABLES:
        if type(rows[table]) is not int or rows[table] <= 0:
            raise ValueError(f"Invalid row count for {table}")
        checked_hash(hashes[table], f"{table} Parquet")
    if rows["lineitem"] < 1_000_000_000:
        raise ValueError("Fixture has fewer than one billion lineitem rows")
    checked_number(manifest.get("scale_factor"), "Scale factor")

    if fixture_dir is not None:
        root = Path(fixture_dir).resolve()
        for table in TABLES:
            relative = paths[table]
            if not isinstance(relative, str):
                raise ValueError(f"Invalid fixture file path for {table}")
            path = (root / relative).resolve()
            if not path.is_relative_to(root) or not path.is_file():
                raise ValueError(f"Unsafe or missing fixture file: {table}")
            digest = hashlib.sha256()
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            if digest.hexdigest() != hashes[table]:
                raise ValueError(f"Fixture checksum mismatch for {table}")
    return fixture_queries(manifest)


def qualify(manifest, benchmark, verification, fixture_dir=None):
    raw_tolerance = verification.get("numeric_tolerance")
    try:
        tolerance = Decimal(str(raw_tolerance))
    except InvalidOperation as error:
        raise ValueError("Invalid reference numeric tolerance") from error
    if (not tolerance.is_finite() or tolerance < 0
            or tolerance > Decimal("0.000001")):
        raise ValueError("Reference numeric tolerance exceeds 0.000001")
    query_hashes = check_fixture(manifest, fixture_dir)
    runs = checked_entries(benchmark, "Benchmark")
    checks = checked_entries(verification, "Verification")
    binary = checked_hash(benchmark.get("binary_sha256"), "RustSpark binary")
    repeat = benchmark.get("repeat")
    warmup = benchmark.get("warmup")
    if type(repeat) is not int or repeat < 3 or type(warmup) is not int or warmup < 1:
        raise ValueError("Use at least three timed samples and one warmup per query")

    total_median_ms = []
    for number in sorted(FULL_SUITE):
        row = runs[number]
        reference = checks[number]
        if row.get("status") != "ok" or reference.get("status") != "match":
            raise ValueError(f"TPC-H query {number} failed execution or DuckDB comparison")
        expected_sha = query_hashes[number]
        if row.get("sql_sha256") != expected_sha or reference.get("sql_sha256") != expected_sha:
            raise ValueError(f"TPC-H query {number} SQL differs from the fixture manifest")
        checked_hash(row.get("result_sha256"), f"Query {number} result")
        samples = row.get("samples_ms")
        if not isinstance(samples, list) or len(samples) != repeat:
            raise ValueError(f"Query {number} is missing timing samples")
        for elapsed in samples:
            checked_number(elapsed, f"Query {number} timing")
        median = checked_number(row.get("median_ms"), f"Query {number} median")
        p95 = checked_number(row.get("p95_ms"), f"Query {number} p95")
        measured_p95 = sorted(samples)[math.ceil(0.95 * len(samples)) - 1]
        if (p95 + 0.01 < median
                or not math.isclose(p95, measured_p95, rel_tol=0, abs_tol=0.01)):
            raise ValueError(f"Query {number} p95 is inconsistent with timing samples")
        if not math.isclose(statistics.median(samples), median, abs_tol=0.01):
            raise ValueError(f"Query {number} median is inconsistent with timing samples")
        if (type(reference.get("expected_rows")) is not int
                or type(reference.get("actual_rows")) is not int
                or reference["expected_rows"] != reference["actual_rows"]):
            raise ValueError(f"Query {number} has inconsistent result row counts")
        total_median_ms.append(median)

    return {
        "qualification": "complete-22-query-cold-tpch",
        "source_lineitem_rows": manifest["rows"]["lineitem"],
        "scale_factor": manifest["scale_factor"],
        "verified_fixture_file_hashes": fixture_dir is not None,
        "independent_duckdb_result_checks": 22,
        "numeric_tolerance": str(tolerance),
        "samples_per_query": repeat,
        "warmups_per_query": warmup,
        "binary_sha256": binary,
        "sum_query_medians_seconds": round(math.fsum(total_median_ms) / 1000, 3),
        "note": "Evidence for this workload only, not a Spark or DuckDB speedup claim",
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("benchmark", type=Path)
    parser.add_argument("verification", type=Path)
    parser.add_argument("--check-files", action="store_true",
                        help="Hash all Parquet inputs against their manifest digests")
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args(argv)
    try:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        benchmark = json.loads(args.benchmark.read_text(encoding="utf-8"))
        verification = json.loads(args.verification.read_text(encoding="utf-8"))
        result = qualify(manifest, benchmark, verification,
                         args.manifest.parent if args.check_files else None)
        rendered = json.dumps(result, indent=2) + "\n"
        if args.json_out:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(rendered, encoding="utf-8")
        else:
            print(rendered, end="")
    except (OSError, ValueError, TypeError) as error:
        parser.exit(1, f"Invalid billion-row evidence: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

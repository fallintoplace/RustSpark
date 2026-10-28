#!/usr/bin/env python3
"""Generate optional large Parquet data, then verify and time RustSpark SQL."""

import argparse
from datetime import datetime, timezone
import importlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import tempfile
import time


DAYS = 365
CUSTOMERS = 10_000
SCHEMA_VERSION = 1


def count_mod(rows, modulus, remainder):
    """Count 0 <= i < rows where i % modulus == remainder."""
    if type(rows) is not int or rows < 1 or not 0 <= remainder < modulus:
        raise ValueError("Invalid count or modular filter")
    return max(0, 1 + (rows - 1 - remainder) // modulus)


def workloads(rows, day):
    if type(rows) is not int or rows < 1 or type(day) is not int or not 0 <= day < DAYS:
        raise ValueError("Invalid row count or day")
    matching = count_mod(rows, DAYS, day)
    a, b = day % 10, (day + DAYS) % 10
    total = ((matching + 1) // 2) * a + (matching // 2) * b
    exact = count_mod(rows, math.lcm(DAYS, CUSTOMERS), day)
    maximum = day + DAYS * (matching - 1) if matching else None
    return {
        "partition_count": (
            f"SELECT COUNT(*) AS n FROM data WHERE day = {day}", [{"n": matching}]
        ),
        "partition_sum": (
            f"SELECT SUM(amount) AS total FROM data WHERE day = {day}",
            [{"total": total if matching else None}],
        ),
        "point_lookup": (
            f"SELECT COUNT(*) AS n FROM data WHERE day = {day} AND customer_id = {day}",
            [{"n": exact}],
        ),
        "partition_top1": (
            f"SELECT MAX(id) AS largest FROM data WHERE day = {day}",
            [{"largest": maximum}],
        ),
        "full_count": ("SELECT COUNT(*) AS n FROM data", [{"n": rows}]),
    }


def parquet_parts(data):
    parts = list(data.rglob("*.parquet"))
    if not parts:
        raise ValueError("Dataset has no Parquet parts")
    return parts


def generate(root, rows):
    """Generate the dataset outside timed samples; publish only on success."""
    if type(rows) is not int or rows < 1:
        raise ValueError("rows must be a positive integer")
    if root.exists() or root.is_symlink():
        raise ValueError("Generation destination must not already exist")
    duckdb = importlib.import_module("duckdb")
    root.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".rustspark-billion-", dir=root.parent))
    try:
        data = staging / "data"
        escaped = data.as_posix().replace("'", "''")
        connection = duckdb.connect(database=":memory:")
        try:
            connection.execute(
                "COPY (SELECT i::BIGINT AS id, CAST(i % 365 AS INTEGER) AS day, "
                "CAST(i % 10000 AS INTEGER) AS customer_id, "
                f"CAST(i % 10 AS INTEGER) AS amount FROM range({rows}) AS t(i)) "
                f"TO '{escaped}' (FORMAT PARQUET, PARTITION_BY (day), COMPRESSION ZSTD)"
            )
        finally:
            connection.close()
        parts = parquet_parts(data)
        metadata = {
            "generator": "rustspark-billion-v1", "schema_version": SCHEMA_VERSION,
            "rows": rows, "day_modulus": DAYS, "customer_modulus": CUSTOMERS,
            "files": len(parts),
        }
        (staging / "manifest.json").write_text(json.dumps(metadata, indent=2) + "\n")
        staging.rename(root)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def load_manifest(root):
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or manifest.get("generator") != "rustspark-billion-v1":
        raise ValueError("Dataset requires a RustSpark billion-row generator manifest")
    rows = manifest.get("rows")
    if type(rows) is not int or rows < 1 or manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("Invalid or incompatible dataset manifest")
    parquet_parts(root / "data")
    return manifest


def query_command(binary, root, sql, partitions):
    command = [str(binary), "--parquet", str((root / "data").resolve()),
               "--parquet-partitions", "day:int32", "--sql", sql,
               "--stdout-format", "json", "--parquet-pushdown-filters",
               "--parquet-reorder-filters"]
    if partitions is not None:
        command += ["--target-partitions", str(partitions)]
    return command


def run_query(binary, root, sql, timeout, partitions):
    started = time.perf_counter()
    result = subprocess.run(query_command(binary, root, sql, partitions),
                            capture_output=True, text=True, timeout=timeout, check=False)
    milliseconds = (time.perf_counter() - started) * 1_000
    if result.returncode:
        raise RuntimeError(f"RustSpark exited {result.returncode}: {result.stderr[-600:]}")
    try:
        result_rows = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    except json.JSONDecodeError as error:
        raise RuntimeError("RustSpark emitted malformed JSON") from error
    return milliseconds, result_rows


def measure(binary, root, rows, day, repeat, warmup, timeout, partitions, only=None):
    if repeat < 1 or warmup < 0 or timeout < 1 or (partitions is not None and partitions < 1):
        raise ValueError("Invalid benchmark timing or parallelism options")
    candidates = workloads(rows, day)
    names = list(candidates) if only is None else list(only)
    if not names or len(set(names)) != len(names) or any(name not in candidates for name in names):
        raise ValueError("Select distinct, known workloads")
    results = []
    for name in names:
        sql, expected = candidates[name]
        item = {"name": name, "sql": sql, "expected": expected}
        try:
            samples = []
            for attempt in range(warmup + repeat):
                milliseconds, actual = run_query(binary, root, sql, timeout, partitions)
                if actual != expected:
                    raise RuntimeError(f"Wrong result for {name}: {actual!r} != {expected!r}")
                if attempt >= warmup:
                    samples.append(round(milliseconds, 3))
            item.update({"status": "ok", "samples_ms": samples,
                         "median_ms": round(statistics.median(samples), 3),
                         "p95_ms": round(sorted(samples)[math.ceil(.95 * len(samples)) - 1], 3)})
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
            item.update({"status": "error", "error": str(error)[-600:]})
        results.append(item)
    return {
        "metric": "cold CLI wall-clock including Parquet registration, planning, execution, output",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(), "cpu_count": os.cpu_count(),
        "rows": rows, "day": day, "warmup": warmup, "repeat": repeat,
        "target_partitions": partitions, "results": results,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True,
                        help="Generated dataset root (contains manifest.json and data/)")
    parser.add_argument("--generate", action="store_true",
                        help="Create dataset first with optional DuckDB Python dependency")
    parser.add_argument("--rows", type=int, default=None,
                        help="Generation row count, default one billion; otherwise verify manifest")
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--day", type=int, default=42)
    parser.add_argument("--target-partitions", type=int)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--only", nargs="+", metavar="NAME")
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.generate:
            generate(args.data_dir, args.rows if args.rows is not None else 1_000_000_000)
        manifest = load_manifest(args.data_dir)
        if args.rows is not None and args.rows != manifest["rows"]:
            raise ValueError("--rows does not match the dataset manifest")
        report = measure(args.binary, args.data_dir, manifest["rows"], args.day,
                         args.repeat, args.warmup, args.timeout, args.target_partitions, args.only)
        if args.json_out:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    except (OSError, ImportError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        parser.exit(1, f"Billion-row benchmark failed: {error}\n")
    for item in report["results"]:
        if item["status"] == "ok":
            print(f"{item['name']}: median={item['median_ms']:.1f}ms, p95={item['p95_ms']:.1f}ms (verified)")
        else:
            print(f"{item['name']}: ERROR {item['error']}")
    return 0 if all(item["status"] == "ok" for item in report["results"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())

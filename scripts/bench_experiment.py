#!/usr/bin/env python3
"""Compare RustSpark execution settings against an independent SQLite result."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import statistics
import subprocess
import tempfile
import time

from bench_rustspark import benchmark_row, output_signature, percentile, write_csv

SCHEMA = "id:int64,city:utf8,amount:int64"
OPTIONS = {
    "--target-partitions", "--batch-size", "--scan-split-min-kb",
    "--csv-schema-rows", "--metadata-fetch-concurrency", "--memory-limit-mb",
}
MAX_ROWS = 2_000_000


def load_experiment(path):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Experiment must be a JSON object")
    name, sql, variants = data.get("name"), data.get("sql"), data.get("variants")
    rows = data.get("rows", 100_000)
    if not isinstance(name, str) or not re.fullmatch(r"[a-z][a-z0-9_]{1,63}", name):
        raise ValueError("Experiment name must be a safe identifier")
    if not isinstance(sql, str) or not sql.lstrip().lower().startswith(("select ", "with ")):
        raise ValueError("Experiment SQL must be a read-only SELECT/WITH statement")
    if not isinstance(variants, list) or not 2 <= len(variants) <= 20:
        raise ValueError("Experiments require 2 to 20 variants")
    if type(rows) is not int or not 1 <= rows <= MAX_ROWS:
        raise ValueError(f"rows must be between 1 and {MAX_ROWS}")
    seen = set()
    for variant in variants:
        if not isinstance(variant, dict):
            raise ValueError("Each variant must be an object")
        label, flags = variant.get("name"), variant.get("flags")
        if not isinstance(label, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", label):
            raise ValueError("Variant names must be safe identifiers")
        if label in seen:
            raise ValueError(f"Duplicate variant name: {label}")
        seen.add(label)
        if not isinstance(flags, list) or len(flags) % 2:
            raise ValueError("Variant flags must be option/value pairs")
        if any(not isinstance(part, str) for part in flags):
            raise ValueError("Variant flag values must be strings")
        options = flags[::2]
        if len(options) != len(set(options)) or any(option not in OPTIONS for option in options):
            raise ValueError(f"Unsupported or repeated flag in {label}")
        for option, raw in zip(options, flags[1::2]):
            if not raw.isdecimal() or int(raw) < 1:
                raise ValueError(f"{option} needs a positive integer")
    return {"name": name, "sql": sql, "rows": rows, "variants": variants}


def sqlite_reference(sql, rows):
    connection = sqlite3.connect(":memory:")
    try:
        connection.execute("CREATE TABLE data (id INTEGER, city TEXT, amount INTEGER)")
        for start in range(0, rows, 4096):
            connection.executemany(
                "INSERT INTO data VALUES (?, ?, ?)",
                ((index, *benchmark_row(index))
                 for index in range(start, min(rows, start + 4096))),
            )
        result = connection.execute(sql)
        keys = [column[0] for column in result.description]
        values = [dict(zip(keys, row)) for row in result.fetchall()]
        return output_signature("\n".join(json.dumps(value) for value in values))
    finally:
        connection.close()


def run_variant(binary, csv, sql, flags, expected_digest, expected_rows, repeat, warmup, timeout):
    command = [
        str(binary), "--csv", str(csv), "--csv-schema", SCHEMA,
        "--sql", sql, "--stdout-format", "json", *flags,
    ]
    samples = []
    for index in range(repeat + warmup):
        started = time.perf_counter()
        result = subprocess.run(
            command, capture_output=True, text=True, check=True, timeout=timeout
        )
        elapsed = (time.perf_counter() - started) * 1000
        digest, values = output_signature(result.stdout)
        if digest != expected_digest or len(values) != expected_rows:
            raise ValueError("RustSpark result differs from SQLite reference")
        if not math.isfinite(elapsed) or elapsed < 0:
            raise ValueError("Invalid elapsed time")
        if index >= warmup:
            samples.append(round(elapsed, 3))
    return {
        "samples_ms": samples,
        "median_ms": round(statistics.median(samples), 3),
        "p95_ms": round(percentile(samples, 0.95), 3),
        "result_sha256": expected_digest,
        "result_rows": expected_rows,
    }


def benchmark(binary, experiment, repeat=3, warmup=1, timeout=120):
    if type(repeat) is not int or repeat < 1 or type(warmup) is not int or warmup < 0:
        raise ValueError("repeat must be positive and warmup nonnegative")
    if type(timeout) is not int or timeout < 1:
        raise ValueError("timeout must be positive")
    rows, sql = experiment["rows"], experiment["sql"]
    expected_digest, expected_values = sqlite_reference(sql, rows)
    with tempfile.TemporaryDirectory(prefix="rustspark-experiment-") as directory:
        csv = Path(directory) / "data.csv"
        write_csv(csv, rows)
        csv_hash = hashlib.sha256(csv.read_bytes()).hexdigest()
        measurements = []
        for variant in experiment["variants"]:
            metrics = run_variant(
                binary, csv, sql, variant["flags"], expected_digest,
                len(expected_values), repeat, warmup, timeout
            )
            measurements.append({"name": variant["name"], "flags": variant["flags"], **metrics})
        return {
            "name": experiment["name"], "sql": sql, "rows": rows,
            "binary": str(binary), "csv_sha256": csv_hash,
            "metric": "Cold CLI startup + typed CSV registration + SQL + JSON output",
            "reference": "SQLite exact result values (unordered rows)",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "repeat": repeat, "warmup": warmup, "results": measurements,
        }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("experiment", type=Path)
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args(argv)
    try:
        report = benchmark(
            args.binary, load_experiment(args.experiment),
            args.repeat, args.warmup, args.timeout
        )
        content = json.dumps(report, indent=2) + "\n"
        if args.json_out:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(content, encoding="utf-8")
        else:
            print(content, end="")
    except (OSError, ValueError, sqlite3.Error, subprocess.SubprocessError) as error:
        parser.exit(1, f"error: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

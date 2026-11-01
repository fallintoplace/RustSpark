#!/usr/bin/env python3
"""Generate TPC-H inputs and require 22 successful RustSpark SQL executions."""

import argparse
import json
import math
from pathlib import Path
import subprocess
import sys

from tpch_run import reject_nonfinite, unique_keys

SCRIPTS = Path(__file__).resolve().parent


def commands(binary, fixture, report, scale, partitions, timeout):
    if (type(scale) not in (int, float) or type(partitions) is not int
            or type(timeout) is not int or partitions < 1 or timeout < 1):
        raise ValueError("Invalid scale factor, partition count or timeout")
    try:
        valid_scale = math.isfinite(scale) and scale > 0
    except OverflowError:
        valid_scale = False
    if not valid_scale:
        raise ValueError("Invalid scale factor, partition count or timeout")
    prepare = [sys.executable, str(SCRIPTS / "tpch_prepare.py"),
               "--scale-factor", str(scale), "--output", str(fixture)]
    run = [sys.executable, str(SCRIPTS / "tpch_run.py"), "--binary", str(binary),
           "--queries-dir", str(fixture / "queries"), "--parquet-dir", str(fixture / "data"),
           "--repeat", "1", "--warmup", "0", "--timeout", str(timeout),
           "--target-partitions", str(partitions), "--json-out", str(report)]
    return prepare, run


def valid_positive_ms(value):
    if type(value) not in (int, float) or value <= 0:
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def complete_query_results(records):
    if not isinstance(records, list) or len(records) != 22:
        return False
    if any(not isinstance(entry, dict) or type(entry.get("id")) is not int
           for entry in records):
        return False
    def valid_measurement(entry):
        median, p95 = entry.get("median_ms"), entry.get("p95_ms")
        rows = entry.get("result_rows")
        hashes = (entry.get("sql_sha256"), entry.get("result_sha256"))
        return (
            valid_positive_ms(median)
            and valid_positive_ms(p95) and p95 >= median
            and type(rows) is int and rows >= 0
            and all(type(value) is str and len(value) == 64
                    and all(char in "0123456789abcdef" for char in value)
                    for value in hashes)
        )

    return ({entry["id"] for entry in records} == set(range(1, 23))
            and all(entry.get("status") == "ok" and valid_measurement(entry)
                    for entry in records))


def smoke_report_succeeded(report, exit_code):
    data = json.loads(report.read_text(encoding="utf-8"),
                      object_pairs_hook=unique_keys,
                      parse_constant=reject_nonfinite)
    if not isinstance(data, dict):
        return False
    records = data.get("results", [])
    return complete_query_results(records) and exit_code == 0


def run_smoke(binary, fixture, report, scale=0.01, partitions=2, timeout=120):
    if fixture.exists():
        raise ValueError("Fixture output must not already exist")
    if report.exists():
        raise ValueError("Benchmark report must not already exist")
    prepare, run = commands(binary, fixture, report, scale, partitions, timeout)
    subprocess.run(prepare, check=True, timeout=max(600, timeout))
    # Keep the JSON report even when the query runner exits nonzero.
    result = subprocess.run(run, check=False, timeout=22 * timeout + 120)
    if not report.is_file():
        raise RuntimeError(f"TPC-H runner did not produce {report} (exit {result.returncode})")
    return smoke_report_succeeded(report, result.returncode)

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("./target/release/rustspark"))
    parser.add_argument("--fixture", required=True, type=Path)
    parser.add_argument("--json-out", required=True, type=Path)
    parser.add_argument("--scale-factor", type=float, default=0.01)
    parser.add_argument("--target-partitions", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args(argv)
    try:
        passed = run_smoke(args.binary, args.fixture, args.json_out,
                           args.scale_factor, args.target_partitions, args.timeout)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        parser.exit(1, f"TPC-H smoke failed: {error}\n")
    print("TPC-H: 22/22 passed" if passed else f"TPC-H: failed; see {args.json_out}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())

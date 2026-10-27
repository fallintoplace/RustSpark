#!/usr/bin/env python3
"""Generate TPC-H inputs and require 22 successful RustSpark SQL executions."""

import argparse
import json
import math
from pathlib import Path
import subprocess
import sys

SCRIPTS = Path(__file__).resolve().parent


def commands(binary, fixture, report, scale, partitions, timeout):
    if not math.isfinite(scale) or scale <= 0 or partitions < 1 or timeout < 1:
        raise ValueError("Invalid scale factor, partition count or timeout")
    prepare = [sys.executable, str(SCRIPTS / "tpch_prepare.py"),
               "--scale-factor", str(scale), "--output", str(fixture)]
    run = [sys.executable, str(SCRIPTS / "tpch_run.py"), "--binary", str(binary),
           "--queries-dir", str(fixture / "queries"), "--parquet-dir", str(fixture / "data"),
           "--repeat", "1", "--warmup", "0", "--timeout", str(timeout),
           "--target-partitions", str(partitions), "--json-out", str(report)]
    return prepare, run


def complete_query_results(records):
    return (isinstance(records, list) and len(records) == 22
            and {entry.get("id") for entry in records} == set(range(1, 23))
            and all(entry.get("status") == "ok" for entry in records))


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
    data = json.loads(report.read_text(encoding="utf-8"))
    records = data.get("results", [])
    return complete_query_results(records) and result.returncode == 0


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

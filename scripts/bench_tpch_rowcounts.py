#!/usr/bin/env python3
"""Check RustSpark row counts match the TPC-H fixture manifest."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

from tpch_run import TABLES, reject_nonfinite, unique_keys


def checked_count(value, message):
    if type(value) is not int or value < 0:
        raise ValueError(message)
    return value


def parse_count(stdout):
    rows = [
        json.loads(line, parse_constant=reject_nonfinite, object_pairs_hook=unique_keys)
        for line in stdout.splitlines() if line.strip()
    ]
    if len(rows) != 1 or not isinstance(rows[0], dict) or set(rows[0]) != {"n"}:
        raise ValueError("Expected one JSON object with only a count column n")
    return checked_count(rows[0]["n"], "Expected a nonnegative integer count")


def count_command(binary, fixture, table):
    if table not in TABLES:
        raise ValueError("Unknown TPC-H table")
    path = fixture / "data" / f"{table}.parquet"
    if not path.is_file():
        raise ValueError(f"Missing table: {path}")
    return [binary, "--parquet-table", f"{table}={path.resolve()}",
            "--sql", f"SELECT COUNT(*) AS n FROM {table}", "--stdout-format", "json"]


MAX_COUNT_OUTPUT_BYTES = 64 * 1024


def execute_count(binary, fixture, table, timeout):
    # A COUNT(*) should return one tiny JSON row. Avoid holding unbounded
    # stdout and stderr in Python memory if the backend is noisy.
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        result = subprocess.run(
            count_command(binary, fixture, table),
            stdout=stdout, stderr=stderr, timeout=timeout, check=False,
        )
        if result.returncode:
            stderr.seek(0, os.SEEK_END)
            stderr.seek(max(0, stderr.tell() - 400))
            detail = stderr.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"COUNT({table}) exited {result.returncode}: {detail}")
        stdout.seek(0, os.SEEK_END)
        if stdout.tell() > MAX_COUNT_OUTPUT_BYTES:
            raise ValueError("COUNT output exceeds 64 KiB budget")
        stdout.seek(0)
        return parse_count(stdout.read().decode("utf-8"))


def compare_table_count(binary, fixture, timeout, table, expected_count):
    count = checked_count(expected_count, f"Invalid expected row count for {table}")
    actual = execute_count(binary, fixture, table, timeout)
    return {"table": table, "expected": count, "actual": actual,
                    "status": "ok" if count == actual else "mismatch"}


def check(binary, fixture, timeout=120):
    if type(timeout) is not int or timeout < 1:
        raise ValueError("Timeout must be a positive integer")
    manifest = json.loads((fixture / "manifest.json").read_text(encoding="utf-8"),
                          object_pairs_hook=unique_keys)
    if not isinstance(manifest, dict):
        raise ValueError("TPC-H row-count manifest must be a JSON object")
    expected = manifest.get("rows")
    if not isinstance(expected, dict) or set(expected) != set(TABLES):
        raise ValueError("Expected row counts for all eight TPC-H tables")
    results = []
    for table in TABLES:
        results.append(compare_table_count(binary, fixture, timeout, table, expected[table]))
    return {"results": results}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args(argv)
    try:
        report = check(args.binary, args.fixture, args.timeout)
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        parser.exit(1, f"TPC-H count validation failed: {error}\n")
    passed = sum(row["status"] == "ok" for row in report["results"])
    print(f"TPC-H row counts: {passed}/8 match")
    return 0 if passed == 8 else 1


if __name__ == "__main__":
    raise SystemExit(main())

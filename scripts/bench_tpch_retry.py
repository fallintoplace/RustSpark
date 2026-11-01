#!/usr/bin/env python3
"""Rerun failed TPC-H query IDs from a prior JSON benchmark report."""

import argparse
import json
import re
from pathlib import Path
import subprocess
import sys


def failed_ids(report):
    if not isinstance(report, dict):
        raise ValueError("TPC-H retry report must be a JSON object")
    results = report.get("results")
    if not isinstance(results, list) or not results:
        raise ValueError("No TPC-H query results found")
    seen, failed = set(), []
    for row in results:
        if not isinstance(row, dict):
            raise ValueError("Invalid TPC-H query result entry")
        number = row.get("id")
        if type(number) is not int or number not in range(1, 23) or number in seen:
            raise ValueError("Invalid or repeated query ID")
        seen.add(number)
        if row.get("status") != "ok":
            failed.append(number)
    return sorted(failed)


def checked_retry_ids(ids):
    """Refuse invalid or duplicate query numbers before invoking the runner."""
    if (not ids or any(type(i) is not int or i not in range(1, 23) for i in ids)
            or len(set(ids)) != len(ids)):
        raise ValueError("Invalid retry query IDs")
    return ids


def retry_command(binary, queries, parquet, ids, report, repeat=1, timeout=120):
    checked_retry_ids(ids)
    if (type(repeat) is not int or repeat < 1
            or type(timeout) is not int or timeout < 1):
        raise ValueError("Invalid retry parameters")
    return [sys.executable, str(Path(__file__).with_name("tpch_run.py")),
            "--binary", binary, "--queries-dir", str(queries), "--parquet-dir", str(parquet),
            "--query-ids", *[str(i) for i in ids], "--repeat", str(repeat),
            "--warmup", "0", "--timeout", str(timeout), "--json-out", str(report)]


def retry_completed(replay, ids, previous=None):
    if not isinstance(replay, dict):
        return False
    new = replay.get("results", [])
    complete = (isinstance(new, list)
                and len(new) == len(ids)
                and all(isinstance(row, dict) and type(row.get("id")) is int
                        for row in new)
                and {row["id"] for row in new} == set(ids)
                and all(row.get("status") == "ok" for row in new))
    if not complete or previous is None:
        return complete

    # Replayed successes must still execute the SQL recorded by the original
    # benchmark. Older failed reports may not include an SQL fingerprint.
    original = {row["id"]: row for row in previous["results"]}
    for row in new:
        expected = original[row["id"]].get("sql_sha256")
        if expected is not None:
            if (not isinstance(expected, str)
                    or re.fullmatch(r"[0-9a-f]{64}", expected) is None
                    or row.get("sql_sha256") != expected):
                return False
    return True


def execute_retry(args):
    if args.previous.resolve() == args.json_out.resolve():
        raise ValueError("Retry output must differ from previous report")
    previous = json.loads(args.previous.read_text(encoding="utf-8"))
    ids = failed_ids(previous)
    if not ids:
        print("All recorded TPC-H queries succeeded; nothing to retry")
        return 0
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    cmd = retry_command(args.binary, args.queries_dir, args.parquet_dir,
                        ids, args.json_out, args.repeat, args.timeout)
    # A previous report must never be reused as fresh retry output.
    args.json_out.unlink(missing_ok=True)
    result = subprocess.run(cmd, check=False)
    # Original runner versions can exit 0 even if some queries failed.
    replay = json.loads(args.json_out.read_text(encoding="utf-8"))
    completed = retry_completed(replay, ids, previous)
    return 0 if completed and result.returncode == 0 else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("previous", type=Path)
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--queries-dir", required=True, type=Path)
    parser.add_argument("--parquet-dir", required=True, type=Path)
    parser.add_argument("--json-out", required=True, type=Path)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args(argv)
    try:
        return execute_retry(args)
    except (OSError, ValueError, TypeError, subprocess.SubprocessError) as error:
        parser.exit(1, f"retry failed: {error}\n")


if __name__ == "__main__":
    raise SystemExit(main())

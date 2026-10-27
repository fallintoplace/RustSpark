#!/usr/bin/env python3
"""Rerun failed TPC-H query IDs from a prior JSON benchmark report."""

import argparse
import json
from pathlib import Path
import subprocess
import sys


def failed_ids(report):
    results = report.get("results")
    if not isinstance(results, list) or not results:
        raise ValueError("No TPC-H query results found")
    seen, failed = set(), []
    for row in results:
        number = row.get("id")
        if type(number) is not int or number not in range(1, 23) or number in seen:
            raise ValueError("Invalid or repeated query ID")
        seen.add(number)
        if row.get("status") != "ok":
            failed.append(number)
    return sorted(failed)


def retry_command(binary, queries, parquet, ids, report, repeat=1, timeout=120):
    if not ids or repeat < 1 or timeout < 1:
        raise ValueError("Invalid retry parameters")
    return [sys.executable, str(Path(__file__).with_name("tpch_run.py")),
            "--binary", binary, "--queries-dir", str(queries), "--parquet-dir", str(parquet),
            "--query-ids", *[str(i) for i in ids], "--repeat", str(repeat),
            "--warmup", "0", "--timeout", str(timeout), "--json-out", str(report)]


def retry_completed(replay, ids):
    new = replay.get("results", [])
    return ({row.get("id") for row in new} == set(ids)
            and len(new) == len(ids)
            and all(row.get("status") == "ok" for row in new))


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
        ids = failed_ids(json.loads(args.previous.read_text(encoding="utf-8")))
        if not ids:
            print("All recorded TPC-H queries succeeded; nothing to retry")
            return 0
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        cmd = retry_command(args.binary, args.queries_dir, args.parquet_dir,
                            ids, args.json_out, args.repeat, args.timeout)
        result = subprocess.run(cmd, check=False)
        # Original runner versions can exit 0 even if some queries failed.
        replay = json.loads(args.json_out.read_text(encoding="utf-8"))
        completed = retry_completed(replay, ids)
    except (OSError, ValueError, TypeError, subprocess.SubprocessError) as error:
        parser.exit(1, f"retry failed: {error}\n")
    return 0 if completed and result.returncode == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

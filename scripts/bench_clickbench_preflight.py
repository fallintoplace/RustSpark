#!/usr/bin/env python3
"""Verify ClickBench's external 43-query SQL file and Parquet input."""

import argparse
import hashlib
import json
from pathlib import Path

from clickbench_run import read_queries


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def validate_parquet_magic(parquet):
    if not parquet.is_file() or parquet.stat().st_size < 8:
        raise ValueError("Parquet input is missing or truncated")
    with parquet.open("rb") as stream:
        header = stream.read(4)
        stream.seek(-4, 2)
        footer = stream.read(4)
    if header != b"PAR1" or footer != b"PAR1":
        raise ValueError("Parquet input is missing its PAR1 magic bytes")


def inspect(parquet, sql_file):
    parquet, sql_file = Path(parquet), Path(sql_file)
    queries = read_queries(sql_file)
    if len(queries) != 43:
        raise ValueError(f"Expected all 43 ClickBench queries, found {len(queries)}")
    validate_parquet_magic(parquet)
    return {
        "queries": len(queries),
        "query_sha256": digest(sql_file),
        "parquet_sha256": digest(parquet),
        "parquet_bytes": parquet.stat().st_size,
        "note": "Input integrity only; query correctness and full Parquet parsing require separate checks",
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet", required=True, type=Path)
    parser.add_argument("--queries", required=True, type=Path)
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.json_out is not None and args.json_out.resolve() in (
            args.parquet.resolve(), args.queries.resolve(),
        ):
            raise ValueError("Report output must differ from Parquet and SQL inputs")
        report = inspect(args.parquet, args.queries)
        output = json.dumps(report, indent=2) + "\n"
        if args.json_out:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(output, encoding="utf-8")
        else:
            print(output, end="")
    except (OSError, ValueError) as error:
        parser.exit(1, f"ClickBench preflight failed: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

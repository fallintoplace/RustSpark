#!/usr/bin/env python3
"""Compare deterministic SQL results against Spark, without changing RustSpark behavior.

Requires PySpark only when running the differential comparison. Standard-library
unit tests cover the comparison machinery in ordinary CI.
"""
import argparse
import datetime
import decimal
import json
import pathlib
import subprocess
import sys
from dataclasses import dataclass

DEFAULT_CASES = pathlib.Path(__file__).resolve().parents[1] / "tests" / "compatibility" / "spark_sql.json"


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[list[object]]


def normalize(value):
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return format(value, "f")
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {str(k): normalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(v) for v in value]
    return value


def fingerprint(result: QueryResult, ordered: bool = False):
    # Preserve column names and value types. Compare row multisets by default:
    # SQL without ORDER BY has no stable row order.
    encoded = [json.dumps(normalize(row), sort_keys=True, ensure_ascii=False) for row in result.rows]
    if not ordered:
        encoded.sort()
    return (result.columns, encoded)


def compare(left: QueryResult, right: QueryResult, ordered: bool = False):
    left_columns, left_rows = fingerprint(left, ordered)
    right_columns, right_rows = fingerprint(right, ordered)
    if left_columns != right_columns:
        return f"columns differ: RustSpark={left_columns!r}, Spark={right_columns!r}"
    if left_rows != right_rows:
        return f"rows differ: RustSpark={left_rows!r}, Spark={right_rows!r}"
    return None


def rustspark_query(binary: str, sql: str):
    common = [binary, "--sql", sql]
    described = subprocess.run(
        common + ["--describe"], capture_output=True, text=True, check=True, timeout=120
    )
    columns = [
        line.split("\t", 1)[0]
        for line in described.stdout.splitlines()
        if line.strip()
    ]
    result = subprocess.run(
        common + ["--stdout-format", "json"],
        capture_output=True, text=True, check=True, timeout=120
    )
    objects = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    rows = [[obj[column] for column in columns] for obj in objects]
    return QueryResult(columns=columns, rows=rows)


def spark_query(session, sql: str):
    df = session.sql(sql)
    columns = df.columns
    rows = [list(row) for row in df.collect()]
    return QueryResult(columns=columns, rows=rows)


def load_cases(path: pathlib.Path):
    cases = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(cases, list) or not cases:
        raise ValueError("Cases must be a nonempty JSON list")
    ids = set()
    for case in cases:
        if not isinstance(case, dict) or not isinstance(case.get("id"), str):
            raise ValueError("Each case needs a string id")
        if not case["id"].strip():
            raise ValueError("SQL case id must not be empty")
        if "ordered" in case and not isinstance(case["ordered"], bool):
            raise ValueError(f"SQL case {case['id']} ordered must be a boolean")
        if case["id"] in ids:
            raise ValueError(f"Duplicate SQL case id: {case['id']}")
        if not isinstance(case.get("sql"), str) or not case["sql"].strip():
            raise ValueError(f"SQL case {case['id']} requires nonempty sql")
        ids.add(case["id"])
    return cases


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rustspark", default="./target/debug/rustspark")
    parser.add_argument("--cases", type=pathlib.Path, default=DEFAULT_CASES)
    parser.add_argument("--spark-master", default="local[2]")
    parser.add_argument("--case", action="append", help="Only run this case ID (repeatable)")
    args = parser.parse_args(argv)

    cases = load_cases(args.cases)
    if args.case:
        unknown = sorted(set(args.case) - {case["id"] for case in cases})
        if unknown:
            parser.error(f"Unknown case IDs: {unknown}")
        cases = [case for case in cases if case["id"] in args.case]

    try:
        from pyspark.sql import SparkSession
    except ImportError:
        parser.error("Install PySpark 4.x before differential comparison: pip install pyspark")

    session = SparkSession.builder.master(args.spark_master).appName("RustSpark SQL compatibility").getOrCreate()
    failed = 0
    try:
        for case in cases:
            try:
                ours = rustspark_query(args.rustspark, case["sql"])
                reference = spark_query(session, case["sql"])
                problem = compare(ours, reference, bool(case.get("ordered", False)))
                if problem is None:
                    print(f"PASS {case['id']}")
                else:
                    failed += 1
                    print(f"DIFF {case['id']}: {problem}")
            except Exception as error:
                failed += 1
                print(f"ERROR {case['id']}: {error}")
    finally:
        session.stop()
    print(f"{len(cases) - failed}/{len(cases)} matched; {failed} differed or errored")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

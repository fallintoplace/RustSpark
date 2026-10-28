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
    # Spark compares +0.0 and -0.0 as equal numeric values. JSON renders
    # their sign differently, so canonicalize signed floating-point zeros.
    if isinstance(value, float) and value == 0.0:
        return 0.0
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, dict):
        return {str(k): normalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(v) for v in value]
    return value


def fingerprint(result: QueryResult, ordered: bool = False):
    # Preserve column names and value types. Compare row multisets by default:
    # SQL without ORDER BY has no stable row order.
    encoded = [json.dumps(normalize(row), sort_keys=True, ensure_ascii=False, allow_nan=False) for row in result.rows]
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


def parse_rustspark_schema_fields(stdout: str) -> list[list[str]]:
    fields = [line.rsplit("\t", 2) for line in stdout.splitlines() if line.strip()]
    if (any(len(parts) != 3 or not parts[0] or not parts[1]
            or parts[2] not in ("NULL", "NOT NULL") for parts in fields)
            or len({parts[0] for parts in fields}) != len(fields)):
        raise ValueError("Invalid RustSpark schema description")
    return fields


def parse_rustspark_columns(stdout: str) -> list[str]:
    return [parts[0] for parts in parse_rustspark_schema_fields(stdout)]


def _unique_json_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate RustSpark JSON key: {key}")
        result[key] = value
    return result


def _reject_nonfinite(value):
    raise ValueError(f"Non-finite RustSpark JSON value: {value}")


def _validate_json_row_shapes(objects, columns):
    # json.loads accepts a huge exponent (1e400) as infinity even when
    # parse_constant rejects the explicit Infinity token.
    json.dumps(objects, allow_nan=False)
    expected = set(columns)
    if len(expected) != len(columns) or any(
        not isinstance(obj, dict) or set(obj) != expected for obj in objects
    ):
        raise ValueError("RustSpark JSON rows must match described columns")


def parse_rustspark_json(stdout: str, columns: list[str]) -> QueryResult:
    objects = [
        json.loads(line, object_pairs_hook=_unique_json_keys, parse_constant=_reject_nonfinite)
        for line in stdout.splitlines() if line.strip()
    ]
    _validate_json_row_shapes(objects, columns)
    rows = [[obj[column] for column in columns] for obj in objects]
    return QueryResult(columns=columns, rows=rows)


def _rustspark_cli_stdout(command: list[str], arguments: list[str]) -> str:
    return subprocess.run(
        command + arguments, capture_output=True, text=True, check=True, timeout=120,
    ).stdout


def rustspark_query(binary: str, sql: str):
    common = [binary, "--sql", sql]
    columns = parse_rustspark_columns(_rustspark_cli_stdout(common, ["--describe"]))
    rows = _rustspark_cli_stdout(common, ["--stdout-format", "json"])
    return parse_rustspark_json(rows, columns)


def spark_query(session, sql: str):
    df = session.sql(sql)
    columns = df.columns
    rows = [list(row) for row in df.collect()]
    return QueryResult(columns=columns, rows=rows)


def validate_case(case, ids: set[str]) -> None:
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


def load_cases(path: pathlib.Path):
    cases = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(cases, list) or not cases:
        raise ValueError("Cases must be a nonempty JSON list")
    ids = set()
    for case in cases:
        validate_case(case, ids)

    # Additional suites live in separate files to avoid changing one large
    # manifest for every Spark compatibility test. Keep a deterministic order.
    fragment_dir = path.with_name(f"{path.stem}_cases")
    if fragment_dir.is_dir():
        for fragment_path in sorted(fragment_dir.glob("*.json")):
            fragment = json.loads(fragment_path.read_text(encoding="utf-8"))
            if isinstance(fragment, dict):
                fragment = [fragment]
            if not isinstance(fragment, list) or not fragment:
                raise ValueError(f"Invalid SQL case fragment: {fragment_path.name}")
            for case in fragment:
                validate_case(case, ids)
            cases.extend(fragment)
    return cases



def compare_case(case, rustspark_runner, spark_runner):
    ours = rustspark_runner(case["sql"])
    reference = spark_runner(case["sql"])
    return compare(ours, reference, case.get("ordered", False))


def run_cases(cases, rustspark_runner, spark_runner):
    failed = 0
    for case in cases:
        try:
            problem = compare_case(case, rustspark_runner, spark_runner)
            if problem is None:
                print(f"PASS {case['id']}")
            else:
                failed += 1
                print(f"DIFF {case['id']}: {problem}")
        except Exception as error:
            failed += 1
            print(f"ERROR {case['id']}: {error}")
    return failed


def build_spark_session(spark_session, master: str):
    return (spark_session.builder.master(master)
            .appName("RustSpark SQL compatibility")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


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

    session = build_spark_session(SparkSession, args.spark_master)
    try:
        failed = run_cases(
            cases,
            lambda sql: rustspark_query(args.rustspark, sql),
            lambda sql: spark_query(session, sql),
        )
    finally:
        session.stop()
    print(f"{len(cases) - failed}/{len(cases)} matched; {failed} differed or errored")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

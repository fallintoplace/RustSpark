#!/usr/bin/env python3
"""Compare table-backed RustSpark SQL with an actual local Spark engine.

This optional differential run discovers semantic differences. It does not
change RustSpark behavior or claim Spark SQL compatibility.
"""
import argparse
import datetime
import decimal
import json
import pathlib
import re
import subprocess
import sys
from dataclasses import dataclass

from spark_compat import QueryResult, compare

ROOT = pathlib.Path(__file__).resolve().parents[1]
CASES = ROOT / "tests/compatibility/spark_typed_sql.json"
FIXTURES = ROOT / "tests/compatibility/fixtures"
TABLES = ("events_left", "events_right")
RUST_SCHEMA = "id:int64,amount:int64,label:utf8,event_time:utf8"
SPARK_SCHEMA = "id LONG, amount LONG, label STRING, event_time STRING"


@dataclass
class TypedResult:
    result: QueryResult
    types: list[str]
    nullables: list[bool] | None = None


def canonical_type(value: str) -> str:
    value = re.sub(r"\s+", "", value).lower()
    value = value.replace("int64", "bigint").replace("int32", "int")
    value = value.replace("int16", "smallint").replace("int8", "tinyint")
    value = value.replace("utf8", "string").replace("float64", "double")
    value = value.replace("float32", "float").replace("boolean", "boolean")
    value = value.replace("date32", "date")
    value = re.sub(r"decimal128\((\d+),(\d+)\)", r"decimal(\1,\2)", value)
    if value.startswith("timestamp("):
        return "timestamp"
    return value


def parse_rustspark_description(stdout: str) -> tuple[list[str], list[str], list[bool]]:
    fields = [line.rsplit("\t", 2) for line in stdout.splitlines() if line.strip()]
    if any(len(parts) != 3 or parts[2] not in ("NULL", "NOT NULL") for parts in fields):
        raise ValueError("Invalid RustSpark schema description")
    return (
        [parts[0] for parts in fields],
        [canonical_type(parts[1]) for parts in fields],
        [parts[2] == "NULL" for parts in fields],
    )


def parse_rustspark_json(stdout: str, columns: list[str]) -> QueryResult:
    objects = [json.loads(line) for line in stdout.splitlines() if line.strip()]
    return QueryResult(columns, [[obj[column] for column in columns] for obj in objects])


def rustspark_query(binary: str, sql: str) -> TypedResult:
    command = [binary]
    for name in TABLES:
        command.extend(["--csv-table", f"{name}={FIXTURES / (name + '.csv')}"])
    command.extend(["--csv-schema", RUST_SCHEMA, "--sql", sql])
    described = subprocess.run(
        command + ["--describe"], capture_output=True, text=True,
        check=True, timeout=120,
    )
    columns, types, nullables = parse_rustspark_description(described.stdout)
    output = subprocess.run(
        command + ["--stdout-format", "json"], capture_output=True, text=True,
        check=True, timeout=120,
    )
    return TypedResult(parse_rustspark_json(output.stdout, columns), types, nullables)


def spark_query(session, sql: str) -> TypedResult:
    dataframe = session.sql(sql)
    return TypedResult(
        QueryResult(dataframe.columns, [list(row) for row in dataframe.collect()]),
        [canonical_type(field.dataType.simpleString()) for field in dataframe.schema.fields],
        [field.nullable for field in dataframe.schema.fields],
    )


def normalized_typed_value(value, logical_type: str):
    """Normalize physical encodings while preserving SQL logical types."""
    if value is None:
        return None
    if logical_type == "timestamp":
        if isinstance(value, datetime.datetime):
            timestamp = value
        else:
            timestamp = datetime.datetime.fromisoformat(
                str(value).replace("Z", "+00:00")
            )
        if timestamp.tzinfo is not None:
            timestamp = timestamp.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return timestamp.isoformat(timespec="microseconds")
    if logical_type == "date":
        if isinstance(value, (datetime.date, datetime.datetime)):
            return value.date().isoformat() if isinstance(value, datetime.datetime) else value.isoformat()
        return datetime.date.fromisoformat(str(value)).isoformat()
    if logical_type.startswith("decimal("):
        # The schema checks precision and scale. Values can be represented as
        # JSON strings (RustSpark) or decimal.Decimal (PySpark).
        return format(decimal.Decimal(str(value)).normalize(), "f")
    return value


def normalize_result(result: TypedResult) -> QueryResult:
    if len(result.types) != len(result.result.columns):
        raise ValueError("Type count differs from column count")
    if any(len(row) != len(result.types) for row in result.result.rows):
        raise ValueError("Row length differs from type count")
    return QueryResult(
        result.result.columns,
        [[normalized_typed_value(value, field_type)
          for value, field_type in zip(row, result.types)]
         for row in result.result.rows],
    )


def compare_typed(
    ours: TypedResult,
    reference: TypedResult,
    ordered: bool,
    check_nullability: bool = False,
) -> str | None:
    if ours.types != reference.types:
        return f"schema differs: RustSpark={ours.types!r}, Spark={reference.types!r}"
    if check_nullability:
        if ours.nullables is None or reference.nullables is None:
            return "schema comparison is missing nullability metadata"
        if ours.nullables != reference.nullables:
            return f"nullability differs: RustSpark={ours.nullables!r}, Spark={reference.nullables!r}"
    return compare(normalize_result(ours), normalize_result(reference), ordered=ordered)


def load_cases(path: pathlib.Path) -> list[dict]:
    from spark_compat import load_cases as validate_cases
    cases = validate_cases(path)
    for case in cases:
        if case.get("ordered", False) not in (True, False):
            raise ValueError(f"Invalid ordering for {case['id']}")
    return cases


def run_cases(cases, rustspark_runner, spark_runner, check_nullability=False) -> tuple[int, list[dict]]:
    results = []
    for case in cases:
        try:
            problem = compare_typed(
                rustspark_runner(case["sql"]),
                spark_runner(case["sql"]),
                case.get("ordered", False),
                check_nullability=check_nullability,
            )
            status = "PASS" if problem is None else "DIFF"
            results.append({"id": case["id"], "status": status, "detail": problem})
            print(f"{status} {case['id']}" + (f": {problem}" if problem else ""))
        except Exception as error:
            results.append({"id": case["id"], "status": "ERROR", "detail": str(error)})
            print(f"ERROR {case['id']}: {error}")
    return sum(item["status"] != "PASS" for item in results), results


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rustspark", default="./target/debug/rustspark")
    parser.add_argument("--cases", type=pathlib.Path, default=CASES)
    parser.add_argument("--spark-master", default="local[2]")
    parser.add_argument("--json-out", type=pathlib.Path)
    parser.add_argument(
        "--check-nullability", action="store_true",
        help="Compare SQL schema nullable flags in addition to values and types",
    )
    args = parser.parse_args(argv)
    cases = load_cases(args.cases)

    try:
        from pyspark.sql import SparkSession
    except ImportError:
        parser.error("PySpark 4.x required: pip install 'pyspark>=4,<5'")
    session = (
        SparkSession.builder.master(args.spark_master)
        .appName("RustSpark table SQL compatibility")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        for name in TABLES:
            (session.read.option("header", "true").schema(SPARK_SCHEMA)
             .csv(str(FIXTURES / (name + ".csv")))
             .createOrReplaceTempView(name))
        failed, results = run_cases(
            cases,
            lambda sql: rustspark_query(args.rustspark, sql),
            lambda sql: spark_query(session, sql),
            check_nullability=args.check_nullability,
        )
    finally:
        session.stop()
    if args.json_out is not None:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps({
            "cases": len(cases), "passed": len(cases) - failed,
            "differed_or_errored": failed,
            "differed": sum(item["status"] == "DIFF" for item in results),
            "errors": sum(item["status"] == "ERROR" for item in results),
            "check_nullability": args.check_nullability,
            "results": results,
        }, indent=2) + "\n", encoding="utf-8")
    print(f"{len(cases) - failed}/{len(cases)} matched; {failed} differed or errored")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

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
import tempfile
import sys
from dataclasses import dataclass

from spark_compat import (
    QueryResult,
    compare,
    parse_rustspark_json,
    parse_rustspark_schema_fields,
)

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
    aliases = {
        "int64": "bigint",
        "int32": "int",
        "int16": "smallint",
        "int8": "tinyint",
        "utf8": "string",
        "float64": "double",
        "float32": "float",
        "date32": "date",
    }
    value = re.sub(r"\b(?:int64|int32|int16|int8|utf8|float64|float32|date32)\b",
                   lambda match: aliases[match.group(0)], value)
    value = re.sub(r"decimal128\((\d+),(\d+)\)", r"decimal(\1,\2)", value)
    if value.startswith("timestamp("):
        return "timestamp"
    return value


def parse_rustspark_description(stdout: str) -> tuple[list[str], list[str], list[bool]]:
    fields = parse_rustspark_schema_fields(stdout)
    return (
        [parts[0] for parts in fields],
        [canonical_type(parts[1]) for parts in fields],
        [parts[2] == "NULL" for parts in fields],
    )


def rustspark_command(
    binary: str, sql: str, input_format: str = "csv",
    parquet_root: pathlib.Path | None = None,
) -> list[str]:
    command = [binary]
    if input_format == "csv":
        for name in TABLES:
            command.extend(["--csv-table", f"{name}={FIXTURES / (name + '.csv')}"])
        command.extend(["--csv-schema", RUST_SCHEMA])
    elif input_format == "parquet":
        if parquet_root is None:
            raise ValueError("Parquet input requires a shared fixture directory")
        for name in TABLES:
            command.extend(["--parquet-table", f"{name}={parquet_root / name}"])
    else:
        raise ValueError(f"Unsupported input format: {input_format}")
    return command + ["--sql", sql]


def _rustspark_stdout(command, arguments):
    return subprocess.run(
        command + arguments, capture_output=True, text=True,
        check=True, timeout=120,
    ).stdout


def rustspark_query(
    binary: str, sql: str, input_format: str = "csv",
    parquet_root: pathlib.Path | None = None,
) -> TypedResult:
    command = rustspark_command(binary, sql, input_format, parquet_root)
    described = _rustspark_stdout(command, ["--describe"])
    columns, types, nullables = parse_rustspark_description(described)
    output = _rustspark_stdout(command, ["--stdout-format", "json"])
    return TypedResult(parse_rustspark_json(output, columns), types, nullables)


def register_spark_sources(
    session, input_format: str, parquet_root: pathlib.Path | None = None,
) -> None:
    if input_format not in ("csv", "parquet"):
        raise ValueError(f"Unsupported input format: {input_format}")
    if input_format == "parquet" and parquet_root is None:
        raise ValueError("Parquet input requires a shared fixture directory")
    for name in TABLES:
        frame = (session.read.option("header", "true").schema(SPARK_SCHEMA)
                 .csv(str(FIXTURES / (name + ".csv"))))
        if input_format == "parquet":
            # Write typed files once. Both engines then read the same Parquet
            # directory instead of accidentally comparing CSV and Parquet.
            output = parquet_root / name
            frame.write.parquet(str(output))
            frame = session.read.parquet(str(output))
        frame.createOrReplaceTempView(name)


def spark_query(session, sql: str) -> TypedResult:
    dataframe = session.sql(sql)
    return TypedResult(
        QueryResult(dataframe.columns, [list(row) for row in dataframe.collect()]),
        [canonical_type(field.dataType.simpleString()) for field in dataframe.schema.fields],
        [field.nullable for field in dataframe.schema.fields],
    )


def normalize_timestamp(value) -> str:
    if isinstance(value, datetime.datetime):
        timestamp = value
    else:
        timestamp = datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if timestamp.tzinfo is not None:
        timestamp = timestamp.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return timestamp.isoformat(timespec="microseconds")


def normalize_date(value) -> str:
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.date().isoformat() if isinstance(value, datetime.datetime) else value.isoformat()
    return datetime.date.fromisoformat(str(value)).isoformat()


def normalized_typed_value(value, logical_type: str):
    """Normalize physical encodings while preserving SQL logical types."""
    if value is None:
        return None
    if logical_type == "timestamp":
        return normalize_timestamp(value)
    if logical_type == "date":
        return normalize_date(value)
    if logical_type.startswith("decimal("):
        # Values may use up to 38 digits, exceeding the default 28-digit
        # Decimal context. Never round away differences during comparison.
        number = decimal.Decimal(str(value))
        with decimal.localcontext() as context:
            context.prec = max(context.prec, len(number.as_tuple().digits))
            return format(number.normalize(), "f")
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
    parser.add_argument("--input-format", choices=("csv", "parquet"), default="csv")
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
        with tempfile.TemporaryDirectory(prefix="rustspark-typed-compat-") as directory:
            parquet_root = pathlib.Path(directory) if args.input_format == "parquet" else None
            register_spark_sources(session, args.input_format, parquet_root)
            failed, results = run_cases(
                cases,
                lambda sql: rustspark_query(
                    args.rustspark, sql, args.input_format, parquet_root,
                ),
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
            "input_format": args.input_format,
            "results": results,
        }, indent=2) + "\n", encoding="utf-8")
    print(f"{len(cases) - failed}/{len(cases)} matched; {failed} differed or errored")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

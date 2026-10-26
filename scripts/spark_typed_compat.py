#!/usr/bin/env python3
"""Compare table-backed RustSpark SQL with an actual local Spark engine.

This optional differential run discovers semantic differences. It does not
change RustSpark behavior or claim Spark SQL compatibility.
"""
import argparse
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


def rustspark_query(binary: str, sql: str) -> TypedResult:
    command = [binary]
    for name in TABLES:
        command.extend(["--csv-table", f"{name}={FIXTURES / (name + '.csv')}"])
    command.extend(["--csv-schema", RUST_SCHEMA, "--sql", sql])
    described = subprocess.run(
        command + ["--describe"], capture_output=True, text=True,
        check=True, timeout=120,
    )
    fields = [line.split("\t") for line in described.stdout.splitlines() if line.strip()]
    if any(len(parts) < 2 for parts in fields):
        raise ValueError("Invalid RustSpark schema description")
    columns = [parts[0] for parts in fields]
    types = [canonical_type(parts[1]) for parts in fields]
    output = subprocess.run(
        command + ["--stdout-format", "json"], capture_output=True, text=True,
        check=True, timeout=120,
    )
    objects = [json.loads(line) for line in output.stdout.splitlines() if line.strip()]
    return TypedResult(
        QueryResult(columns, [[obj[column] for column in columns] for obj in objects]),
        types,
    )


def spark_query(session, sql: str) -> TypedResult:
    dataframe = session.sql(sql)
    return TypedResult(
        QueryResult(dataframe.columns, [list(row) for row in dataframe.collect()]),
        [canonical_type(field.dataType.simpleString()) for field in dataframe.schema.fields],
    )


def compare_typed(ours: TypedResult, reference: TypedResult, ordered: bool) -> str | None:
    issue = compare(ours.result, reference.result, ordered=ordered)
    if issue is not None:
        return issue
    if ours.types != reference.types:
        return f"schema differs: RustSpark={ours.types!r}, Spark={reference.types!r}"
    return None


def load_cases(path: pathlib.Path) -> list[dict]:
    from spark_compat import load_cases as validate_cases
    cases = validate_cases(path)
    for case in cases:
        if case.get("ordered", False) not in (True, False):
            raise ValueError(f"Invalid ordering for {case['id']}")
    return cases


def run_cases(cases, rustspark_runner, spark_runner) -> tuple[int, list[dict]]:
    results = []
    for case in cases:
        try:
            problem = compare_typed(
                rustspark_runner(case["sql"]),
                spark_runner(case["sql"]),
                case.get("ordered", False),
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
        )
    finally:
        session.stop()
    if args.json_out is not None:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps({
            "cases": len(cases), "passed": len(cases) - failed,
            "differed_or_errored": failed, "results": results,
        }, indent=2) + "\n", encoding="utf-8")
    print(f"{len(cases) - failed}/{len(cases)} matched; {failed} differed or errored")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

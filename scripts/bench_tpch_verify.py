#!/usr/bin/env python3
"""Compare RustSpark TPC-H query outputs with DuckDB on identical Parquet data.

Requires the optional duckdb Python package. This is an interoperability check,
not an official TPC-H certification.
"""

import argparse
import datetime
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import subprocess

from clickbench_run import has_top_level_order_by
from tpch_run import TABLES, command_for, input_arguments, query_paths, unique_keys, reject_nonfinite


def numeric(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return None
    return None


def same_value(actual, expected, tolerance):
    if actual is None or expected is None:
        return actual is None and expected is None
    if isinstance(actual, bool) or isinstance(expected, bool):
        return type(actual) is bool and type(expected) is bool and actual == expected
    a, b = numeric(actual), numeric(expected)
    # DataFusion may serialize DECIMAL as a JSON string; only coerce strings
    # when the other engine has a numeric value.
    if a is None and isinstance(actual, str) and b is not None:
        try:
            a = Decimal(actual)
        except InvalidOperation:
            pass
    if b is None and isinstance(expected, str) and a is not None:
        try:
            b = Decimal(expected)
        except InvalidOperation:
            pass
    if a is not None and b is not None:
        return abs(a - b) <= tolerance * max(Decimal(1), abs(b))
    if isinstance(expected, (datetime.date, datetime.datetime)):
        expected = expected.isoformat(sep=" ") if isinstance(expected, datetime.datetime) else expected.isoformat()
    return actual == expected


def same_row(actual, expected, tolerance):
    return (actual.keys() == expected.keys()
            and all(same_value(actual[key], expected[key], tolerance) for key in actual))


def equal_rows(actual, expected, tolerance, ordered):
    if len(actual) != len(expected):
        return False
    if ordered:
        return all(same_row(a, b, tolerance) for a, b in zip(actual, expected))
    remaining = list(expected)
    for row in actual:
        matching = next((i for i, candidate in enumerate(remaining)
                         if same_row(row, candidate, tolerance)), None)
        if matching is None:
            return False
        remaining.pop(matching)
    return not remaining


def parse_rows(stdout):
    rows = []
    for line in stdout.splitlines():
        if line.strip():
            row = json.loads(line, parse_constant=reject_nonfinite,
                             object_pairs_hook=unique_keys, parse_float=Decimal)
            if not isinstance(row, dict):
                raise ValueError("RustSpark emitted a non-object result row")
            rows.append(row)
    return rows


def reference_connection(directory):
    import duckdb
    con = duckdb.connect(":memory:")
    for table in TABLES:
        path = directory / f"{table}.parquet"
        if not path.is_file():
            con.close()
            raise ValueError(f"Missing Parquet table: {path}")
        escaped = str(path.resolve()).replace("'", "''")
        con.execute(f'CREATE VIEW "{table}" AS SELECT * FROM read_parquet('
                    f"'{escaped}')")
    return con


def verify(binary, parquet, queries, timeout=120, partitions=2, tolerance=Decimal("0.000001")):
    if timeout <= 0 or partitions < 1 or not tolerance.is_finite() or tolerance < 0:
        raise ValueError("Invalid verification parameters")
    args = input_arguments(parquet)
    con = reference_connection(parquet)
    results = []
    try:
        for number, path in queries:
            result = {"id": number}
            try:
                sql = path.read_text(encoding="utf-8")
                cursor = con.execute(sql)
                columns = [item[0] for item in cursor.description]
                if len(set(columns)) != len(columns):
                    raise ValueError("Duplicate SQL output names in DuckDB")
                reference = [dict(zip(columns, row)) for row in cursor.fetchall()]
                completed = subprocess.run(
                    command_for(binary, args, path, partitions),
                    capture_output=True, text=True, timeout=timeout, check=False,
                )
                if completed.returncode:
                    raise RuntimeError(f"RustSpark exited {completed.returncode}: {completed.stderr[-500:]}")
                actual = parse_rows(completed.stdout)
                matched = equal_rows(
                    actual, reference, tolerance,
                    ordered=has_top_level_order_by(sql),
                )
                result.update({"status": "match" if matched else "mismatch",
                               "expected_rows": len(reference), "actual_rows": len(actual)})
            except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
                result.update({"status": "error", "error": str(error)[-600:]})
            results.append(result)
    finally:
        con.close()
    return {"metric": "row-value comparison on identical Parquet inputs",
            "numeric_tolerance": str(tolerance),
            "results": results}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--parquet-dir", required=True, type=Path)
    parser.add_argument("--queries-dir", required=True, type=Path)
    parser.add_argument("--query-ids", nargs="+", type=int)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--target-partitions", type=int, default=2)
    parser.add_argument("--numeric-tolerance", type=Decimal, default=Decimal("0.000001"))
    parser.add_argument("--json-out", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        report = verify(args.binary, args.parquet_dir,
                        query_paths(args.queries_dir, args.query_ids),
                        args.timeout, args.target_partitions, args.numeric_tolerance)
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    except (OSError, ValueError, ImportError) as error:
        parser.exit(1, f"verification failed: {error}\n")
    passed = sum(entry["status"] == "match" for entry in report["results"])
    print(f"TPC-H reference: {passed}/{len(report['results'])} queries match")
    return 0 if passed == len(report["results"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())

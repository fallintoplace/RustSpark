#!/usr/bin/env python3
"""Smoke-test the actual release binary, including a cross-process SQL write."""
import argparse
import json
import pathlib
import subprocess
import tempfile


def execute(binary: pathlib.Path, *args: str) -> str:
    result = subprocess.run(
        [str(binary), *args], capture_output=True, text=True, check=False, timeout=60
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"rustspark failed ({result.returncode}): {args!r}\n{result.stderr}"
        )
    return result.stdout


def rows(binary: pathlib.Path, *args: str) -> list[dict]:
    data = execute(binary, *args, "--stdout-format", "json")
    return [json.loads(line) for line in data.splitlines() if line]


def expect_rows(binary: pathlib.Path, expected: list[dict], *args: str) -> None:
    actual = rows(binary, *args)
    if actual != expected:
        raise AssertionError(f"Expected SQL rows {expected!r}, got {actual!r}")


def check_catalog_lifecycle(binary: pathlib.Path) -> None:
    steps = [
        ("CREATE TABLE items (id BIGINT)", None),
        ("INSERT INTO items VALUES (1), (2)", None),
        ("SELECT SUM(id) AS total FROM items", [{"total": 3}]),
        ("ALTER TABLE items RENAME TO entries", None),
        ("SELECT COUNT(*) AS n FROM entries", [{"n": 2}]),
        ("DROP TABLE entries", None),
    ]
    with tempfile.TemporaryDirectory() as directory:
        for sql, expected in steps:
            command = ("--database-dir", directory, "--sql", sql)
            if expected is None:
                execute(binary, *command)
            else:
                expect_rows(binary, expected, *command)
        if (pathlib.Path(directory) / "entries").exists():
            raise AssertionError("DROP TABLE left the entries directory behind")



def check_file_workflows(binary: pathlib.Path) -> None:
    """Exercise the released CSV, parameter, join and Parquet paths."""
    with tempfile.TemporaryDirectory() as directory:
        root = pathlib.Path(directory)
        orders = root / "orders.csv"
        rates = root / "rates.csv"
        orders.write_text("city,amount\nBerlin,7\nMunich,3\nBerlin,2\n", encoding="utf-8")
        rates.write_text("city,amount\nBerlin,1\nMunich,2\n", encoding="utf-8")
        schema = "city:utf8,amount:int64"

        expect_rows(
            binary,
            [{"city": "Berlin", "weighted": 9}, {"city": "Munich", "weighted": 6}],
            "--csv-table", f"orders={orders}",
            "--csv-table", f"rates={rates}",
            "--csv-schema", schema,
            "--sql", "SELECT o.city, SUM(o.amount * r.amount) AS weighted "
                     "FROM orders o JOIN rates r USING(city) "
                     "GROUP BY o.city ORDER BY o.city",
        )
        expect_rows(
            binary, [{"total": 10}],
            "--csv", str(orders), "--csv-schema", schema,
            "--sql", "SELECT SUM(amount) AS total FROM data WHERE amount >= $1",
            "--param", "int64:3",
        )

        exported = root / "parquet-result"
        execute(
            binary, "--csv", str(orders), "--csv-schema", schema,
            "--sql", "SELECT city, amount FROM data",
            "--output-parquet", str(exported),
        )
        expect_rows(
            binary,
            [{"city": "Berlin", "total": 9}, {"city": "Munich", "total": 3}],
            "--parquet", str(exported),
            "--sql", "SELECT city, SUM(amount) AS total FROM data "
                     "GROUP BY city ORDER BY city",
        )


def check_query_errors(binary: pathlib.Path) -> None:
    result = subprocess.run(
        [str(binary), "--sql", "SELECT * FROM missing_v1_release_table",
         "--stdout-format", "json"],
        capture_output=True, text=True, check=False, timeout=60,
    )
    if result.returncode == 0:
        raise AssertionError("Invalid SQL returned a successful exit code")
    if result.stdout.strip():
        raise AssertionError(f"Invalid SQL wrote misleading success output: {result.stdout!r}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit(f"Binary not found: {binary}")

    expect_rows(binary, [{"answer": 42}], "--sql", "SELECT 40 + 2 AS answer")
    check_catalog_lifecycle(binary)
    check_file_workflows(binary)
    check_query_errors(binary)
    print(f"Release binary smoke checks passed: {binary.name}")


if __name__ == "__main__":
    main()

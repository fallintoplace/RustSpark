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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit(f"Binary not found: {binary}")

    expect_rows(binary, [{"answer": 42}], "--sql", "SELECT 40 + 2 AS answer")
    check_catalog_lifecycle(binary)
    print(f"Release binary smoke checks passed: {binary.name}")


if __name__ == "__main__":
    main()

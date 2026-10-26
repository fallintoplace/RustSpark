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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit(f"Binary not found: {binary}")

    assert rows(binary, "--sql", "SELECT 40 + 2 AS answer") == [{"answer": 42}]
    with tempfile.TemporaryDirectory() as directory:
        root = ["--database-dir", directory]
        execute(binary, *root, "--sql", "CREATE TABLE items (id BIGINT)")
        execute(binary, *root, "--sql", "INSERT INTO items VALUES (1), (2)")
        result = rows(binary, *root, "--sql", "SELECT SUM(id) AS total FROM items")
        assert result == [{"total": 3}], result
        execute(binary, *root, "--sql", "ALTER TABLE items RENAME TO entries")
        result = rows(binary, *root, "--sql", "SELECT COUNT(*) AS n FROM entries")
        assert result == [{"n": 2}], result
    print(f"Release binary smoke checks passed: {binary.name}")


if __name__ == "__main__":
    main()

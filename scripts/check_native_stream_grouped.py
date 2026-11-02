#!/usr/bin/env python3
"""Check native streaming GROUP BY against SQLite on a deterministic CSV fixture.

Examples:
  python3 scripts/check_native_stream_grouped.py --rows 100000
  python3 scripts/check_native_stream_grouped.py --rows 1000000 --runs 3
"""
import argparse
import csv
import io
import json
from pathlib import Path
import sqlite3
import statistics
import subprocess
import tempfile
import time

REGIONS = ("Berlin", "Paris", "London", "Tokyo", "Rome", "Madrid", "Prague", "Oslo")
CATEGORIES = ("books", "food", "tech", "home", "travel", "health")
FIELDS = ("region", "category", "status", "amount")
RESULT_FIELDS = ("region", "category", "total", "n")
SQL = """
SELECT region, category, SUM(amount) AS total, COUNT(amount) AS n
FROM events
WHERE status = 0 AND amount BETWEEN 1000 AND 9000
GROUP BY region, category
HAVING SUM(amount) > 500000
ORDER BY region, category
LIMIT 20
""".strip()


def fact_row(index: int) -> tuple[str, str, int, int]:
    """Derive each column without randomness, timestamps or external data."""
    return (
        REGIONS[(index * 7 + index // 97) % len(REGIONS)],
        CATEGORIES[(index * 11 + index // 19) % len(CATEGORIES)],
        (index // 13) % 3,
        500 + (index * 17) % 9000,
    )


def make_fixture(path: Path, connection: sqlite3.Connection, rows: int) -> None:
    connection.execute(
        "CREATE TABLE events (region TEXT NOT NULL, category TEXT NOT NULL,"
        " status INTEGER NOT NULL, amount INTEGER NOT NULL)"
    )
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(FIELDS)
        pending = []
        for index in range(rows):
            row = fact_row(index)
            writer.writerow(row)
            pending.append(row)
            if len(pending) == 4096:
                connection.executemany("INSERT INTO events VALUES (?, ?, ?, ?)", pending)
                pending.clear()
        if pending:
            connection.executemany("INSERT INTO events VALUES (?, ?, ?, ?)", pending)
    connection.commit()


def sqlite_result(connection: sqlite3.Connection) -> list[tuple[str, ...]]:
    return [tuple(map(str, row)) for row in connection.execute(SQL)]


def decode_rustspark_output(data: str) -> list[tuple[str, ...]]:
    result = list(csv.reader(io.StringIO(data, newline="")))
    if not result or tuple(result[0]) != RESULT_FIELDS:
        raise ValueError(f"Unexpected output header: {result[0] if result else None!r}")
    if len(result) > 21 or any(len(row) != len(RESULT_FIELDS) for row in result[1:]):
        raise ValueError("Unexpected output row count or width")
    return [tuple(row) for row in result[1:]]


def run_engine(binary: str, path: Path, timeout: int) -> tuple[float, list[tuple[str, ...]]]:
    cmd = [
        binary, "--csv", str(path), "--table", "events",
        "--schema", "region:text!,category:text!,status:int!,amount:int!",
        "--sql", SQL,
    ]
    start = time.perf_counter()
    process = subprocess.run(cmd, capture_output=True, text=True,
                             encoding="utf-8", timeout=timeout, check=False)
    elapsed_ms = (time.perf_counter() - start) * 1000
    if process.returncode:
        raise RuntimeError(
            f"rustspark-stream exited {process.returncode}: "
            f"{process.stderr[:4000].strip()}"
        )
    return elapsed_ms, decode_rustspark_output(process.stdout)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="rustspark-stream",
                        help="Path or executable name of the native streaming CLI")
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=180,
                        help="Per-run timeout in seconds")
    args = parser.parse_args()
    if not 1 <= args.rows <= 10_000_000:
        parser.error("--rows must be between 1 and 10,000,000")
    if not 1 <= args.runs <= 20:
        parser.error("--runs must be between 1 and 20")
    if not 1 <= args.timeout <= 3600:
        parser.error("--timeout must be between 1 and 3600")
    with tempfile.TemporaryDirectory(prefix="rustspark-grouped-oracle-") as directory:
        path = Path(directory) / "events.csv"
        connection = sqlite3.connect(":memory:")
        try:
            make_fixture(path, connection, args.rows)
            expected = sqlite_result(connection)
        finally:
            connection.close()
        samples = []
        for _ in range(args.runs):
            elapsed_ms, actual = run_engine(args.binary, path, args.timeout)
            if actual != expected:
                first = next(
                    ((index, a, b) for index, (a, b)
                     in enumerate(zip(actual, expected)) if a != b), None)
                raise RuntimeError(
                    f"Results differ: native={len(actual)} SQLite={len(expected)}; "
                    f"first mismatch={first}"
                )
            samples.append(round(elapsed_ms, 3))
        print(json.dumps({
            "engine": "rustspark-stream", "oracle": "SQLite",
            "rows": args.rows, "result_rows": len(expected), "samples_ms": samples,
            "median_full_cli_ms": statistics.median(samples), "correct": True,
            "note": "CLI includes startup, scan, execution and output; SQLite is correctness oracle only",
        }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

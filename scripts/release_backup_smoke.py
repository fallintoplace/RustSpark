#!/usr/bin/env python3
"""Qualify independent local-database backups in the packaged RustSpark binary."""

import argparse
from pathlib import Path
import subprocess
import tempfile

from release_smoke import execute, expect_rows


def check_manifest(snapshot: Path) -> None:
    manifest = (snapshot / ".rustspark-backup-manifest").read_text(encoding="utf-8")
    if manifest.splitlines() != ["rustspark-backup-v1", "first", "second"]:
        raise AssertionError(f"Unexpected database backup manifest: {manifest!r}")
    for table in ("first", "second"):
        parts = list((snapshot / table).glob("*.parquet"))
        if not parts or any(not part.is_file() for part in parts):
            raise AssertionError(f"Missing backed-up Parquet parts for {table}")


def expect_backup_rejected(binary: Path, database: Path, destination: Path) -> None:
    result = subprocess.run(
        [
            str(binary), "--database-dir", str(database),
            "--backup-database", str(destination),
        ],
        capture_output=True, text=True, check=False, timeout=60,
    )
    if result.returncode == 0:
        raise AssertionError(f"Invalid backup destination accepted: {destination}")


def _create_fixture_database(binary: Path, database: Path) -> None:
    for sql in (
        "CREATE TABLE first AS SELECT 1 AS n",
        "CREATE TABLE second AS SELECT 20 AS n",
        "INSERT INTO first VALUES (2)",
    ):
        execute(binary, "--database-dir", str(database), "--sql", sql)



def check_backup(binary: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="rustspark-backup-release-") as root:
        database = Path(root) / "db"
        snapshot = Path(root) / "snapshot"

        _create_fixture_database(binary, database)

        execute(binary, "--database-dir", str(database),
                "--backup-database", str(snapshot))
        check_manifest(snapshot)
        expect_rows(binary, [{"total": 3}], "--database-dir", str(snapshot),
                    "--sql", "SELECT SUM(n) AS total FROM first")
        expect_rows(binary, [{"total": 20}], "--database-dir", str(snapshot),
                    "--sql", "SELECT SUM(n) AS total FROM second")

        # A backup must remain readable after the live database changes.
        execute(binary, "--database-dir", str(database),
                "--sql", "INSERT INTO first VALUES (100)")
        execute(binary, "--database-dir", str(database),
                "--sql", "DROP TABLE second")
        expect_rows(binary, [{"total": 103}], "--database-dir", str(database),
                    "--sql", "SELECT SUM(n) AS total FROM first")
        expect_rows(binary, [{"total": 3}], "--database-dir", str(snapshot),
                    "--sql", "SELECT SUM(n) AS total FROM first")
        expect_rows(binary, [{"total": 20}], "--database-dir", str(snapshot),
                    "--sql", "SELECT SUM(n) AS total FROM second")

        expect_backup_rejected(binary, database, snapshot)
        expect_backup_rejected(binary, database, database / "nested")
        if (database / "nested").exists():
            raise AssertionError("Rejected nested backup left a directory")
        check_manifest(snapshot)
        expect_rows(binary, [{"total": 3}], "--database-dir", str(snapshot),
                    "--sql", "SELECT SUM(n) AS total FROM first")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    args = parser.parse_args(argv)
    binary = args.binary.resolve()
    if not binary.is_file():
        parser.error(f"Binary not found: {binary}")
    check_backup(binary)
    print(f"Packaged binary backup smoke checks passed: {binary.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

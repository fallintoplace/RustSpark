"""Produce a Polars LazyFrame from a RustSpark Arrow IPC file."""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import re
import subprocess
import tempfile
from typing import Iterator


TABLE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def append_named_tables(command, flag, tables):
    for name, path in sorted((tables or {}).items()):
        if not isinstance(name, str) or TABLE_NAME.fullmatch(name) is None:
            raise ValueError(f"Invalid SQL table name: {name!r}")
        if not isinstance(path, (str, Path)) or not str(path):
            raise ValueError("Named file paths must not be empty")
        command.extend([flag, f"{name}={path}"])


def build_command(sql: str, output: Path, *, binary: str = "rustspark",
                  csv: str | None = None, parquet: str | None = None,
                  query_timeout: int | None = None,
                  csv_tables: dict[str, str | Path] | None = None,
                  parquet_tables: dict[str, str | Path] | None = None) -> list[str]:
    if not sql.strip():
        raise ValueError("SQL cannot be blank")
    if (csv is not None or parquet is not None) and (csv_tables or parquet_tables):
        raise ValueError("Named and unnamed file inputs cannot be mixed")
    if csv is not None and parquet is not None:
        raise ValueError("Use only one file source")
    command = [binary, "--sql", sql, "--output-arrow", str(output)]
    if csv is not None:
        command.extend(["--csv", csv])
    if parquet is not None:
        command.extend(["--parquet", parquet])
    append_named_tables(command, "--csv-table", csv_tables)
    append_named_tables(command, "--parquet-table", parquet_tables)
    if query_timeout is not None:
        if query_timeout <= 0:
            raise ValueError("query_timeout must be positive")
        command.extend(["--query-timeout", str(query_timeout)])
    return command


@contextmanager
def rustspark_lazy_frame(sql: str, *, binary: str = "rustspark",
                         csv: str | None = None, parquet: str | None = None,
                         query_timeout: int | None = None,
                         csv_tables: dict[str, str | Path] | None = None,
                         parquet_tables: dict[str, str | Path] | None = None,
                         temp_dir: str | None = None) -> Iterator[object]:
    """Export Arrow once and provide a lazy Polars scan while the file exists.

    The frame must be collected or processed inside the context manager.
    """
    import polars as pl  # Optional dependency.

    with tempfile.TemporaryDirectory(dir=temp_dir, prefix="rustspark-polars-") as directory:
        result = Path(directory) / "result.arrow"
        command = build_command(
            sql, result, binary=binary, csv=csv, parquet=parquet,
            query_timeout=query_timeout,
            csv_tables=csv_tables, parquet_tables=parquet_tables,
        )
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
        if completed.returncode:
            raise RuntimeError(completed.stderr.strip() or "RustSpark query failed")
        yield pl.scan_ipc(result)

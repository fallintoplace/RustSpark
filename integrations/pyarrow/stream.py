"""Iterate over RustSpark Arrow IPC batches without buffering the full result."""
from __future__ import annotations

import os
import re
from pathlib import Path
import subprocess
from typing import Iterator


TABLE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\\Z")


def add_named_inputs(command, flag, tables):
    for name, path in sorted((tables or {}).items()):
        if not isinstance(name, str) or TABLE_NAME.fullmatch(name) is None:
            raise ValueError(f"Invalid SQL table name: {name!r}")
        file_path = os.fspath(path)
        if not file_path:
            raise ValueError("Named input paths must not be empty")
        command.extend([flag, f"{name}={file_path}"])


def build_command(
    sql: str, *,
    binary: str = "rustspark",
    csv: str | Path | None = None,
    parquet: str | Path | None = None,
    json_input: str | Path | None = None,
    max_rows: int | None = None,
    query_timeout: int | None = None,
    csv_tables: dict[str, str | Path] | None = None,
    parquet_tables: dict[str, str | Path] | None = None,
    json_tables: dict[str, str | Path] | None = None,
) -> list[str]:
    if not sql.strip():
        raise ValueError("SQL must not be empty")
    selected = [("csv", csv), ("parquet", parquet), ("json", json_input)]
    if sum(value is not None for _, value in selected) > 1:
        raise ValueError("Exactly zero or one file input is allowed")
    if any(value is not None for _, value in selected) and any(
        (csv_tables, parquet_tables, json_tables)
    ):
        raise ValueError("Named and unnamed file sources cannot be combined")
    if max_rows is not None and max_rows < 0:
        raise ValueError("max_rows must be nonnegative")
    if query_timeout is not None and query_timeout <= 0:
        raise ValueError("query_timeout must be positive")
    command = [binary, "--sql", sql, "--stdout-format", "arrow"]
    for format_name, path in selected:
        if path is not None:
            command.extend([f"--{format_name}", os.fspath(path)])
    add_named_inputs(command, "--csv-table", csv_tables)
    add_named_inputs(command, "--parquet-table", parquet_tables)
    add_named_inputs(command, "--json-table", json_tables)
    if max_rows is not None:
        command.extend(["--max-rows", str(max_rows)])
    if query_timeout is not None:
        command.extend(["--query-timeout", str(query_timeout)])
    return command


def iter_arrow_batches(
    sql: str, *,
    binary: str = "rustspark",
    csv: str | Path | None = None,
    parquet: str | Path | None = None,
    json_input: str | Path | None = None,
    max_rows: int | None = None,
    query_timeout: int | None = None,
    csv_tables: dict[str, str | Path] | None = None,
    parquet_tables: dict[str, str | Path] | None = None,
    json_tables: dict[str, str | Path] | None = None,
) -> Iterator[object]:
    """Yield pyarrow.RecordBatch values from Arrow IPC stdout.

    Keep the iterator open until all batches have been consumed. Closing the
    generator early terminates the RustSpark process.
    """
    import pyarrow as pa  # Optional dependency, not needed for RustSpark itself.

    command = build_command(
        sql, binary=binary, csv=csv, parquet=parquet, json_input=json_input,
        max_rows=max_rows, query_timeout=query_timeout,
        csv_tables=csv_tables, parquet_tables=parquet_tables, json_tables=json_tables,
    )
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert process.stdout is not None
        with pa.ipc.open_stream(process.stdout) as reader:
            for batch in reader:
                yield batch
        status = process.wait()
        if status:
            assert process.stderr is not None
            stderr = process.stderr.read().decode("utf-8", errors="replace").strip()
            raise RuntimeError(stderr or f"RustSpark exited with status {status}")
    finally:
        if process.poll() is None:
            process.terminate()
        if process.stdout:
            process.stdout.close()
        if process.stderr:
            process.stderr.close()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()

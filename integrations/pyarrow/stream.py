"""Iterate over RustSpark Arrow IPC batches without buffering the full result."""
from __future__ import annotations

import os
import re
from pathlib import Path
import subprocess
import tempfile
from typing import Iterator


TABLE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


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
    database_dir: str | Path | None = None,
    csv: str | Path | None = None,
    parquet: str | Path | None = None,
    json_input: str | Path | None = None,
    max_rows: int | None = None,
    query_timeout: int | None = None,
    csv_tables: dict[str, str | Path] | None = None,
    parquet_tables: dict[str, str | Path] | None = None,
    json_tables: dict[str, str | Path] | None = None,
    named_params: dict[str, str] | None = None,
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
    if max_rows is not None and (type(max_rows) is not int or max_rows < 0):
        raise ValueError("max_rows must be a nonnegative integer")
    if query_timeout is not None and (type(query_timeout) is not int or query_timeout <= 0):
        raise ValueError("query_timeout must be a positive integer")
    command = [binary, "--sql", sql, "--stdout-format", "arrow"]
    if database_dir is not None:
        command.extend(["--database-dir", os.fspath(database_dir)])
    for format_name, path in selected:
        if path is not None:
            command.extend([f"--{format_name}", os.fspath(path)])
    add_named_inputs(command, "--csv-table", csv_tables)
    add_named_inputs(command, "--parquet-table", parquet_tables)
    add_named_inputs(command, "--json-table", json_tables)
    if named_params:
        if len(named_params) > 64:
            raise ValueError("Too many SQL parameters")
        for name in sorted(named_params):
            value = named_params[name]
            if not isinstance(name, str) or TABLE_NAME.fullmatch(name) is None:
                raise ValueError("Invalid SQL parameter name")
            if not isinstance(value, str) or ":" not in value or len(value) > 8192:
                raise ValueError("SQL parameters must be bounded typed values")
            command.extend(["--named-param", f"{name}={value}"])
    if max_rows is not None:
        command.extend(["--max-rows", str(max_rows)])
    if query_timeout is not None:
        command.extend(["--query-timeout", str(query_timeout)])
    return command


def iter_arrow_batches(
    sql: str, *,
    binary: str = "rustspark",
    database_dir: str | Path | None = None,
    csv: str | Path | None = None,
    parquet: str | Path | None = None,
    json_input: str | Path | None = None,
    max_rows: int | None = None,
    query_timeout: int | None = None,
    csv_tables: dict[str, str | Path] | None = None,
    parquet_tables: dict[str, str | Path] | None = None,
    json_tables: dict[str, str | Path] | None = None,
    named_params: dict[str, str] | None = None,
) -> Iterator[object]:
    """Yield pyarrow.RecordBatch values from Arrow IPC stdout.

    Keep the iterator open until all batches have been consumed. Closing the
    generator early terminates the RustSpark process.
    """
    import pyarrow as pa  # Optional dependency, not needed for RustSpark itself.

    command = build_command(
        sql, binary=binary, database_dir=database_dir,
        csv=csv, parquet=parquet, json_input=json_input,
        max_rows=max_rows, query_timeout=query_timeout,
        csv_tables=csv_tables, parquet_tables=parquet_tables, json_tables=json_tables,
        named_params=named_params,
    )
    # Spool stderr to disk so verbose diagnostics cannot block the Arrow stream.
    with tempfile.TemporaryFile() as error_log:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=error_log)
        try:
            assert process.stdout is not None
            with pa.ipc.open_stream(process.stdout) as reader:
                for batch in reader:
                    yield batch
            status = process.wait()
            if status:
                error_log.seek(0, os.SEEK_END)
                error_log.seek(max(0, error_log.tell() - 8192))
                stderr = error_log.read().decode("utf-8", errors="replace").strip()
                raise RuntimeError(stderr or f"RustSpark exited with status {status}")
        finally:
            if process.poll() is None:
                process.terminate()
            if process.stdout:
                process.stdout.close()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

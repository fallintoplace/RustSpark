"""Produce a Polars LazyFrame from a RustSpark Arrow IPC file."""
from __future__ import annotations

import math
import os
import re
import signal
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Mapping

TABLE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _add_named_tables(command: list[str], flag: str,
                      tables: Mapping[str, str | Path] | None) -> None:
    tables = tables or {}
    if any(not isinstance(name, str) or TABLE_NAME.fullmatch(name) is None for name in tables):
        raise ValueError("Named tables require simple ASCII SQL identifiers")
    for name in sorted(tables):
        path = tables[name]
        if not isinstance(path, (str, os.PathLike)):
            raise ValueError("Named source paths must be strings or filesystem paths")
        file_path = os.fspath(path)
        if not isinstance(file_path, str) or not file_path:
            raise ValueError("Named source paths must be nonempty strings")
        command.extend([flag, f"{name}={file_path}"])


def build_command(sql: str, output: Path, *, binary: str = "rustspark",
                  csv: str | Path | None = None,
                  parquet: str | Path | None = None,
                  json_input: str | Path | None = None,
                  query_timeout: int | None = None,
                  database_dir: str | Path | None = None,
                  max_rows: int | None = None,
                  csv_tables: Mapping[str, str | Path] | None = None,
                  parquet_tables: Mapping[str, str | Path] | None = None,
                  json_tables: Mapping[str, str | Path] | None = None) -> list[str]:
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("SQL cannot be blank")
    if sum(source is not None for source in (csv, parquet, json_input)) > 1:
        raise ValueError("Use only one unnamed file source")
    if any(source is not None for source in (csv, parquet, json_input)) and (csv_tables or parquet_tables or json_tables):
        raise ValueError("Named and unnamed file inputs cannot be mixed")
    if query_timeout is not None and (type(query_timeout) is not int or query_timeout <= 0):
        raise ValueError("query_timeout must be a positive integer")
    if max_rows is not None and (type(max_rows) is not int or max_rows < 1):
        raise ValueError("max_rows must be a positive integer")

    command = [binary, "--sql", sql, "--output-arrow", str(output)]
    if database_dir is not None:
        command.extend(["--database-dir", os.fspath(database_dir)])
    for flag, source in (("--csv", csv), ("--parquet", parquet), ("--json", json_input)):
        if source is not None:
            command.extend([flag, os.fspath(source)])
    _add_named_tables(command, "--csv-table", csv_tables)
    _add_named_tables(command, "--parquet-table", parquet_tables)
    _add_named_tables(command, "--json-table", json_tables)
    if query_timeout is not None:
        command.extend(["--query-timeout", str(query_timeout)])
    if max_rows is not None:
        command.extend(["--max-rows", str(max_rows)])
    return command


def _run_export(command: list[str], *, stderr, timeout: float | None) -> int:
    """Bound the entire export process group, including spawned children."""
    with subprocess.Popen(
        command, stdout=subprocess.DEVNULL, stderr=stderr,
        start_new_session=(os.name == "posix"),
    ) as process:
        try:
            return process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            if os.name == "posix":
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            else:
                process.kill()
            process.wait()
            raise


@contextmanager
def rustspark_lazy_frame(sql: str, *, binary: str = "rustspark",
                         csv: str | Path | None = None,
                         parquet: str | Path | None = None,
                         json_input: str | Path | None = None,
                         query_timeout: int | None = None,
                         database_dir: str | Path | None = None,
                         max_rows: int | None = None,
                         csv_tables: Mapping[str, str | Path] | None = None,
                         parquet_tables: Mapping[str, str | Path] | None = None,
                         json_tables: Mapping[str, str | Path] | None = None,
                         temp_dir: str | None = None,
                         process_timeout: float | None = None) -> Iterator[object]:
    """Export Arrow once and provide a lazy Polars scan while the file exists.

    The frame must be collected or processed inside the context manager.
    """
    if process_timeout is not None and (
        type(process_timeout) not in (int, float) or
        not math.isfinite(process_timeout) or process_timeout <= 0
    ):
        raise ValueError("process_timeout must be finite and positive")
    import polars as pl  # Optional dependency.

    with tempfile.TemporaryDirectory(dir=temp_dir, prefix="rustspark-polars-") as directory:
        result = Path(directory) / "result.arrow"
        command = build_command(
            sql, result, binary=binary, csv=csv, parquet=parquet,
            json_input=json_input, query_timeout=query_timeout,
            database_dir=database_dir, max_rows=max_rows,
            csv_tables=csv_tables, parquet_tables=parquet_tables,
            json_tables=json_tables,
        )
        # A backend can emit arbitrarily many diagnostics. Keep these off
        # the Python heap and surface only a bounded tail on failure.
        with tempfile.TemporaryFile() as error_log:
            try:
                returncode = _run_export(command, stderr=error_log,
                                         timeout=process_timeout)
            except subprocess.TimeoutExpired as error:
                raise RuntimeError("RustSpark Arrow export exceeded process timeout") from error
            if returncode:
                error_log.seek(0, os.SEEK_END)
                error_log.seek(max(0, error_log.tell() - 65_536))
                detail = error_log.read().decode("utf-8", errors="replace").strip()
                raise RuntimeError(detail or "RustSpark query failed")
        yield pl.scan_ipc(result)

"""Run local RustSpark SQL and return a pandas DataFrame using Arrow IPC."""
from __future__ import annotations

import math
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Mapping

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
                  max_rows: int = 100_000, query_timeout: int | None = None,
                  database_dir: str | Path | None = None,
                  csv_tables: Mapping[str, str | Path] | None = None,
                  parquet_tables: Mapping[str, str | Path] | None = None,
                  json_tables: Mapping[str, str | Path] | None = None,
                  named_params: Mapping[str, str] | None = None) -> list[str]:
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("SQL cannot be empty")
    if sum(source is not None for source in (csv, parquet, json_input)) > 1:
        raise ValueError("Specify only one unnamed CSV, Parquet, or JSON source")
    if any(source is not None for source in (csv, parquet, json_input)) and (csv_tables or parquet_tables or json_tables):
        raise ValueError("Named and unnamed file inputs cannot be mixed")
    if type(max_rows) is not int or max_rows < 1:
        raise ValueError("max_rows must be a positive integer")
    if query_timeout is not None and (type(query_timeout) is not int or query_timeout <= 0):
        raise ValueError("query_timeout must be a positive integer")

    command = [binary, "--sql", sql, "--output-arrow", str(output),
               "--max-rows", str(max_rows)]
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

    params = named_params or {}
    if len(params) > 64 or any(not isinstance(name, str) or TABLE_NAME.fullmatch(name) is None
                               for name in params):
        raise ValueError("Invalid SQL parameter name or parameter count")
    for name in sorted(params):
        value = params[name]
        if not isinstance(value, str) or ":" not in value or len(value) > 8192:
            raise ValueError("SQL parameters must be bounded typed values")
        command.extend(["--named-param", f"{name}={value}"])
    return command


def query_dataframe(sql: str, *, binary: str = "rustspark",
                    csv: str | Path | None = None,
                    parquet: str | Path | None = None,
                    json_input: str | Path | None = None,
                    max_rows: int = 100_000, query_timeout: int | None = None,
                    database_dir: str | Path | None = None,
                    csv_tables: Mapping[str, str | Path] | None = None,
                    parquet_tables: Mapping[str, str | Path] | None = None,
                    json_tables: Mapping[str, str | Path] | None = None,
                    named_params: Mapping[str, str] | None = None,
                    process_timeout: float | None = None):
    """Load a bounded SQL result as pandas, preserving Arrow logical types.

    A DataFrame resides fully in Python memory. Prefer streaming for large data.
    """
    if process_timeout is not None and (
        type(process_timeout) not in (int, float) or
        not math.isfinite(process_timeout) or process_timeout <= 0
    ):
        raise ValueError("process_timeout must be finite and positive")
    import pyarrow as pa

    with tempfile.TemporaryDirectory(prefix="rustspark-pandas-") as directory:
        output = Path(directory) / "result.arrow"
        command = build_command(
            sql, output, binary=binary, csv=csv, parquet=parquet,
            json_input=json_input, max_rows=max_rows, query_timeout=query_timeout,
            database_dir=database_dir, csv_tables=csv_tables,
            parquet_tables=parquet_tables, json_tables=json_tables,
            named_params=named_params,
        )
        # Spool diagnostics to disk rather than retaining unbounded stderr
        # in memory during an Arrow export that produces no stdout.
        with tempfile.TemporaryFile() as error_log:
            try:
                result = subprocess.run(command, stdout=subprocess.DEVNULL,
                                        stderr=error_log, check=False,
                                        timeout=process_timeout)
            except subprocess.TimeoutExpired as error:
                raise RuntimeError("RustSpark Arrow export exceeded process timeout") from error
            if result.returncode:
                error_log.seek(0, os.SEEK_END)
                error_log.seek(max(0, error_log.tell() - 65_536))
                detail = error_log.read().decode("utf-8", errors="replace").strip()
                raise RuntimeError(detail or "RustSpark query failed")
        with pa.memory_map(str(output), "r") as source:
            return pa.ipc.open_file(source).read_all().to_pandas()

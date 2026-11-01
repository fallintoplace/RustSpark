"""Run local RustSpark SQL and return a pandas DataFrame using Arrow IPC."""
from __future__ import annotations

import math
import os
import re
import subprocess
import signal
import tempfile
import threading
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
        if not isinstance(database_dir, (str, os.PathLike)):
            raise ValueError("database_dir must be a nonempty text path")
        database_path = os.fspath(database_dir)
        if not isinstance(database_path, str) or not database_path:
            raise ValueError("database_dir must be a nonempty text path")
        command.extend(["--database-dir", database_path])
    for flag, source in (("--csv", csv), ("--parquet", parquet), ("--json", json_input)):
        if source is not None:
            if not isinstance(source, (str, os.PathLike)):
                raise ValueError("Unnamed inputs must be nonempty text paths")
            path = os.fspath(source)
            if not isinstance(path, str) or not path:
                raise ValueError("Unnamed inputs must be nonempty text paths")
            command.extend([flag, path])
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
        if not isinstance(value, str) or ":" not in value or len(value.encode("utf-8")) > 8192:
            raise ValueError("SQL parameters must be bounded typed values")
        command.extend(["--named-param", f"{name}={value}"])
    return command


def _export_arrow(command: list[str], error_log, process_timeout: float | None,
                  output: Path, max_arrow_bytes: int) -> int:
    """Monitor temporary IPC files so native writes cannot grow without bound."""
    process = subprocess.Popen(
        command, stdout=subprocess.DEVNULL, stderr=error_log,
        start_new_session=(os.name == "posix"),
    )
    stop_monitor = threading.Event()
    oversized = threading.Event()
    monitor = None

    def kill_group():
        if os.name == "posix":
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        elif process.poll() is None:
            process.kill()

    def watch_size():
        while not stop_monitor.wait(0.02):
            try:
                size = output.stat().st_size
            except FileNotFoundError:
                continue
            if size > max_arrow_bytes:
                oversized.set()
                kill_group()
                return

    try:
        monitor = threading.Thread(target=watch_size, daemon=True)
        monitor.start()
        status = process.wait(timeout=process_timeout)
        if oversized.is_set() or (output.exists() and output.stat().st_size > max_arrow_bytes):
            raise RuntimeError("RustSpark Arrow export exceeds max_arrow_bytes")
        return status
    except BaseException:
        if process.poll() is None:
            kill_group()
            process.wait()
        raise
    finally:
        stop_monitor.set()
        if monitor is not None and monitor.is_alive():
            monitor.join(timeout=1)



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
                    process_timeout: float | None = None,
                    max_arrow_bytes: int = 512 * 1024 * 1024):
    """Load a bounded SQL result as pandas, preserving Arrow logical types.

    A DataFrame resides fully in Python memory. Prefer streaming for large data.
    """
    if process_timeout is not None and (
        type(process_timeout) not in (int, float) or
        not math.isfinite(process_timeout) or process_timeout <= 0
    ):
        raise ValueError("process_timeout must be finite and positive")
    if type(max_arrow_bytes) is not int or max_arrow_bytes <= 0:
        raise ValueError("max_arrow_bytes must be a positive integer")
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
                status = _export_arrow(command, error_log, process_timeout, output, max_arrow_bytes)
            except subprocess.TimeoutExpired as error:
                raise RuntimeError("RustSpark Arrow export exceeded process timeout") from error
            except OSError as error:
                raise RuntimeError("Unable to start RustSpark Arrow export") from error
            if status:
                error_log.seek(0, os.SEEK_END)
                error_log.seek(max(0, error_log.tell() - 65_536))
                detail = error_log.read().decode("utf-8", errors="replace").strip()
                raise RuntimeError(detail or "RustSpark query failed")
        with pa.memory_map(str(output), "r") as source:
            return pa.ipc.open_file(source).read_all().to_pandas()

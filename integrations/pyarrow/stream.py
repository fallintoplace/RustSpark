"""Iterate over RustSpark Arrow IPC batches without buffering the full result."""
from __future__ import annotations

import os
import signal
import math
import threading
import re
from pathlib import Path
import subprocess
import tempfile
from typing import Iterator


TABLE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def add_named_inputs(command, flag, tables):
    tables = tables or {}
    for name in tables:
        if not isinstance(name, str) or TABLE_NAME.fullmatch(name) is None:
            raise ValueError(f"Invalid SQL table name: {name!r}")
    for name, path in sorted(tables.items()):
        if not isinstance(path, (str, os.PathLike)):
            raise ValueError("Named input paths must be text paths")
        file_path = os.fspath(path)
        if not isinstance(file_path, str) or not file_path:
            raise ValueError("Named input paths must be nonempty text paths")
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
) -> list[str]:
    if not isinstance(sql, str) or not sql.strip():
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
        if not isinstance(database_dir, (str, os.PathLike)):
            raise ValueError("database_dir must be a nonempty text path")
        database_path = os.fspath(database_dir)
        if not isinstance(database_path, str) or not database_path:
            raise ValueError("database_dir must be a nonempty text path")
        command.extend(["--database-dir", database_path])
    for format_name, path in selected:
        if path is not None:
            if not isinstance(path, (str, os.PathLike)):
                raise ValueError("Unnamed input paths must be nonempty text paths")
            file_path = os.fspath(path)
            if not isinstance(file_path, str) or not file_path:
                raise ValueError("Unnamed input paths must be nonempty text paths")
            command.extend([f"--{format_name}", file_path])
    add_named_inputs(command, "--csv-table", csv_tables)
    add_named_inputs(command, "--parquet-table", parquet_tables)
    add_named_inputs(command, "--json-table", json_tables)
    if max_rows is not None:
        command.extend(["--max-rows", str(max_rows)])
    if query_timeout is not None:
        command.extend(["--query-timeout", str(query_timeout)])
    return command


class _BudgetedIPCStream:
    """Bound Arrow IPC bytes read from a child process, including framing."""

    def __init__(self, source, limit: int):
        self.source = source
        self.remaining = limit

    def _account(self, count: int) -> None:
        if count > self.remaining:
            raise ValueError("RustSpark Arrow IPC output exceeds byte budget")
        self.remaining -= count

    def read(self, size: int = -1):
        # Probe one byte beyond the budget to distinguish exact-fit EOF
        # from an oversized stream. Never request unbounded input.
        if size is None or size < 0:
            size = self.remaining + 1
        else:
            size = min(size, self.remaining + 1)
        data = self.source.read(size)
        self._account(len(data))
        return data

    def readinto(self, buffer):
        request = memoryview(buffer)[:self.remaining + 1]
        if hasattr(self.source, "readinto"):
            count = self.source.readinto(request)
            if count is None:
                return None
        else:
            data = self.source.read(len(request))
            count = len(data)
            request[:count] = data
        self._account(count)
        return count

    def readable(self):
        return True


def iter_arrow_batches(
    sql: str, *,
    binary: str = "rustspark",
    database_dir: str | Path | None = None,
    csv: str | Path | None = None,
    parquet: str | Path | None = None,
    json_input: str | Path | None = None,
    max_rows: int | None = None,
    query_timeout: int | None = None,
    process_timeout: float | None = None,
    max_ipc_bytes: int = 512 * 1024 * 1024,
    csv_tables: dict[str, str | Path] | None = None,
    parquet_tables: dict[str, str | Path] | None = None,
    json_tables: dict[str, str | Path] | None = None,
) -> Iterator[object]:
    """Yield pyarrow.RecordBatch values from Arrow IPC stdout.

    Keep the iterator open until all batches have been consumed. Closing the
    generator early terminates the RustSpark process.
    """
    if type(max_ipc_bytes) is not int or max_ipc_bytes <= 0:
        raise ValueError("max_ipc_bytes must be a positive integer")
    if process_timeout is not None and (
        type(process_timeout) not in (int, float)
        or not math.isfinite(process_timeout) or process_timeout <= 0
    ):
        raise ValueError("process_timeout must be finite and positive")
    import pyarrow as pa  # Optional dependency, not needed for RustSpark itself.

    command = build_command(
        sql, binary=binary, database_dir=database_dir,
        csv=csv, parquet=parquet, json_input=json_input,
        max_rows=max_rows, query_timeout=query_timeout,
        csv_tables=csv_tables, parquet_tables=parquet_tables, json_tables=json_tables,
    )
    # Spool stderr to disk so verbose diagnostics cannot block the Arrow stream.
    with tempfile.TemporaryFile() as error_log:
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=error_log,
            start_new_session=(os.name == "posix"),
        )
        completed = False
        deadline_reached = threading.Event()
        finished = threading.Event()
        watchdog = None

        def terminate_group():
            if os.name == "posix":
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            elif process.poll() is None:
                process.kill()

        def on_deadline():
            # A child can exit while descendants still hold Arrow stdout open.
            # Kill the process group until decoding has fully completed.
            if not finished.is_set():
                deadline_reached.set()
                terminate_group()

        if process_timeout is not None:
            watchdog = threading.Timer(process_timeout, on_deadline)
            watchdog.daemon = True
            watchdog.start()

        try:
            assert process.stdout is not None
            with pa.ipc.open_stream(_BudgetedIPCStream(process.stdout, max_ipc_bytes)) as reader:
                for batch in reader:
                    yield batch
            status = process.wait()
            completed = True
            finished.set()
            if deadline_reached.is_set():
                raise TimeoutError("RustSpark Arrow stream exceeded process timeout")
            if status:
                error_log.seek(0, os.SEEK_END)
                error_log.seek(max(0, error_log.tell() - 8192))
                stderr = error_log.read().decode("utf-8", errors="replace").strip()
                raise RuntimeError(stderr or f"RustSpark exited with status {status}")
        except Exception as error:
            if deadline_reached.is_set() and not isinstance(error, TimeoutError):
                raise TimeoutError("RustSpark Arrow stream exceeded process timeout") from error
            raise
        finally:
            finished.set()
            if watchdog is not None:
                watchdog.cancel()
            if not completed:
                # Stop descendants that inherited stdout if iteration ends early.
                terminate_group()
            if process.stdout:
                process.stdout.close()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

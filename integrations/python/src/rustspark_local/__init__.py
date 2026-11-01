"""Local subprocess client for RustSpark, without SQL text interpolation."""

from __future__ import annotations

import json
import math
import os
import re
import subprocess
import signal
import threading
import tempfile

from .subprocess_budget import run_bounded
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Iterator

__all__ = ["RustSpark", "RustSparkError"]

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class RustSparkError(RuntimeError):
    """The RustSpark CLI reported a query or execution failure."""


def _unique_json_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON result key: {key}")
        result[key] = value
    return result


def _reject_nonfinite(value):
    raise ValueError(f"Nonfinite JSON value: {value}")


def _finite_json_float(value):
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"Nonfinite JSON number: {value}")
    return parsed


def _text_path(value, label: str) -> str:
    if not isinstance(value, (str, os.PathLike)):
        raise ValueError(f"{label} must be a nonempty text path")
    path = os.fspath(value)
    if not isinstance(path, str) or not path:
        raise ValueError(f"{label} must be a nonempty text path")
    return path


@dataclass(frozen=True)
class RustSpark:
    binary: str | os.PathLike[str] = "rustspark"
    database_dir: str | os.PathLike[str] | None = None

    def _command(
        self,
        sql: str | None,
        *,
        sql_file: str | os.PathLike[str] | None = None,
        input: str | os.PathLike[str] | None = None,
        csv: str | os.PathLike[str] | None = None,
        parquet: str | os.PathLike[str] | None = None,
        json_input: str | os.PathLike[str] | None = None,
        csv_tables: Mapping[str, str | os.PathLike[str]] | None = None,
        parquet_tables: Mapping[str, str | os.PathLike[str]] | None = None,
        json_tables: Mapping[str, str | os.PathLike[str]] | None = None,
        named_params: Mapping[str, str] | None = None,
        max_rows: int | None = None,
        query_timeout: int | None = None,
        process_timeout: float | None = None,
        max_output_bytes: int = 64 * 1024 * 1024,
    ) -> list[str]:
        """Validate query options and build the native CLI argv."""
        if sql_file is None:
            if not isinstance(sql, str) or not sql.strip():
                raise ValueError("SQL cannot be blank")
        else:
            if (sql is not None and (not isinstance(sql, str) or sql.strip())) or (
                isinstance(sql_file, (str, bytes)) and not sql_file
            ):
                raise ValueError("Provide exactly one inline SQL statement or sql_file")
            _text_path(sql_file, "sql_file")
        unnamed = {"input": input, "csv": csv, "parquet": parquet, "json": json_input}
        named = {"csv-table": csv_tables, "parquet-table": parquet_tables, "json-table": json_tables}
        if sum(value is not None for value in unnamed.values()) > 1:
            raise ValueError("Provide only one unnamed input")
        if any(value is not None for value in unnamed.values()) and any(named.values()):
            raise ValueError("Named and unnamed inputs cannot be mixed")
        if max_rows is not None and (type(max_rows) is not int or max_rows < 0):
            raise ValueError("max_rows must be a nonnegative integer")
        if query_timeout is not None and (type(query_timeout) is not int or query_timeout <= 0):
            raise ValueError("query_timeout must be a positive integer")
        if process_timeout is not None and (
            type(process_timeout) not in (int, float)
            or not math.isfinite(process_timeout) or process_timeout <= 0
        ):
            raise ValueError("process_timeout must be finite and positive")

        if type(max_output_bytes) is not int or max_output_bytes <= 0:
            raise ValueError("max_output_bytes must be a positive integer")

        command = [_text_path(self.binary, "binary")]
        if sql_file is None:
            command.extend(["--sql", sql])
        else:
            command.extend(["--sql-file", _text_path(sql_file, "sql_file")])
        command.extend(["--stdout-format", "json-array"])
        if self.database_dir is not None:
            command.extend(["--database-dir", _text_path(self.database_dir, "database_dir")])
        for flag, value in unnamed.items():
            if value is not None:
                command.extend([f"--{flag}", _text_path(value, f"{flag} input")])
        for flag, values in named.items():
            values = values or {}
            for table in values:
                if not isinstance(table, str) or not IDENTIFIER.fullmatch(table):
                    raise ValueError(f"Invalid table name: {table!r}")
            for table, path in sorted(values.items()):
                if not isinstance(path, (str, os.PathLike)):
                    raise ValueError("Named source paths must be text paths")
                file_path = os.fspath(path)
                if not isinstance(file_path, str) or not file_path:
                    raise ValueError("Named source paths must be nonempty text paths")
                command.extend([f"--{flag}", f"{table}={file_path}"])
        params = named_params or {}
        if len(params) > 64:
            raise ValueError("At most 64 typed SQL parameters are supported")
        for name in params:
            if not isinstance(name, str) or not IDENTIFIER.fullmatch(name):
                raise ValueError(f"Invalid SQL parameter name: {name!r}")
        for name in sorted(params):
            typed_value = params[name]
            if (not isinstance(typed_value, str)
                or ":" not in typed_value or len(typed_value.encode("utf-8")) > 8192):
                raise ValueError(f"Use bounded type:value for parameter {name!r}")
            command.extend(["--named-param", f"{name}={typed_value}"])
        if max_rows is not None:
            command.extend(["--max-rows", str(max_rows)])
        if query_timeout is not None:
            command.extend(["--query-timeout", str(query_timeout)])

        return command

    def query(
        self,
        sql: str | None,
        *,
        sql_file: str | os.PathLike[str] | None = None,
        input: str | os.PathLike[str] | None = None,
        csv: str | os.PathLike[str] | None = None,
        parquet: str | os.PathLike[str] | None = None,
        json_input: str | os.PathLike[str] | None = None,
        csv_tables: Mapping[str, str | os.PathLike[str]] | None = None,
        parquet_tables: Mapping[str, str | os.PathLike[str]] | None = None,
        json_tables: Mapping[str, str | os.PathLike[str]] | None = None,
        named_params: Mapping[str, str] | None = None,
        max_rows: int | None = None,
        query_timeout: int | None = None,
        process_timeout: float | None = None,
        max_output_bytes: int = 64 * 1024 * 1024,
    ) -> list[dict[str, Any]]:
        """Execute SQL and return JSON rows. Use max_rows to bound returned data.

        Named parameters are typed CLI values, e.g. {"min": "int64:10"}.
        The CLI binds them without substituting text into the SQL statement.
        """
        command = self._command(
            sql, sql_file=sql_file, input=input, csv=csv, parquet=parquet,
            json_input=json_input, csv_tables=csv_tables,
            parquet_tables=parquet_tables, json_tables=json_tables,
            named_params=named_params, max_rows=max_rows,
            query_timeout=query_timeout, process_timeout=process_timeout,
            max_output_bytes=max_output_bytes,
        )

        try:
            completed = run_bounded(
                command, timeout=process_timeout, max_output_bytes=max_output_bytes,
            )
        except subprocess.TimeoutExpired as exc:
            raise RustSparkError(f"RustSpark exceeded process timeout: {process_timeout}s") from exc
        except ValueError as exc:
            raise RustSparkError(f"RustSpark output admission failed: {exc}") from exc
        except OSError as exc:
            raise RustSparkError(f"Unable to start RustSpark: {exc}") from exc
        if completed.returncode:
            message = completed.stderr.strip() or f"RustSpark exited with {completed.returncode}"
            raise RustSparkError(message)
        try:
            rows = json.loads(
                completed.stdout, object_pairs_hook=_unique_json_keys,
                parse_constant=_reject_nonfinite, parse_float=_finite_json_float,
            )
        except (json.JSONDecodeError, ValueError, RecursionError) as exc:
            raise RustSparkError("RustSpark returned invalid JSON") from exc
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            raise RustSparkError("RustSpark returned an unexpected JSON result shape")
        return rows

    def iter_rows(
        self, sql: str | None, *,
        process_timeout: float | None = None,
        max_output_bytes: int = 64 * 1024 * 1024,
        max_row_bytes: int = 16 * 1024 * 1024,
        **options: Any,
    ) -> Iterator[dict[str, Any]]:
        """Stream strict JSON object rows with a hard deadline and byte budgets.

        Call close() when ending iteration early to stop the native process.
        No result rows are held beyond the current line.
        """
        if type(max_row_bytes) is not int or not 1 <= max_row_bytes <= 64 * 1024 * 1024:
            raise ValueError("max_row_bytes must be within 1 and 64 MiB")
        command = self._command(
            sql, process_timeout=process_timeout,
            max_output_bytes=max_output_bytes, **options,
        )
        output_flag = command.index("--stdout-format")
        command[output_flag + 1] = "json"
        with tempfile.TemporaryFile() as error_log:
            try:
                process = subprocess.Popen(
                    command, stdout=subprocess.PIPE, stderr=error_log,
                    start_new_session=(os.name == "posix"),
                )
            except OSError as error:
                raise RustSparkError("Unable to start RustSpark NDJSON stream") from error

            expired = threading.Event()
            finished = threading.Event()

            def stop_child() -> None:
                if os.name == "posix":
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                elif process.poll() is None:
                    process.kill()

            def on_timeout() -> None:
                if not finished.is_set() and process.poll() is None:
                    expired.set()
                    stop_child()

            watchdog = None
            if process_timeout is not None:
                watchdog = threading.Timer(process_timeout, on_timeout)
                watchdog.daemon = True
                watchdog.start()
            total = 0
            try:
                assert process.stdout is not None
                while True:
                    line = process.stdout.readline(max_row_bytes + 1)
                    if not line:
                        break
                    total += len(line)
                    if len(line) > max_row_bytes:
                        raise RustSparkError("RustSpark NDJSON row exceeds max_row_bytes")
                    if total > max_output_bytes:
                        raise RustSparkError("RustSpark NDJSON output exceeds max_output_bytes")
                    try:
                        row = json.loads(
                            line.decode("utf-8"), object_pairs_hook=_unique_json_keys,
                            parse_constant=_reject_nonfinite, parse_float=_finite_json_float,
                        )
                    except (ValueError, RecursionError, UnicodeDecodeError) as error:
                        raise RustSparkError("Invalid RustSpark NDJSON row") from error
                    if not isinstance(row, dict):
                        raise RustSparkError("RustSpark NDJSON row must be an object")
                    if expired.is_set():
                        raise RustSparkError("RustSpark NDJSON process timeout")
                    yield row

                status = process.wait()
                if expired.is_set():
                    raise RustSparkError("RustSpark NDJSON process timeout")
                if status != 0:
                    error_log.seek(0, os.SEEK_END)
                    error_log.seek(max(0, error_log.tell() - 8192))
                    detail = error_log.read().decode("utf-8", errors="replace").strip()
                    raise RustSparkError(detail or f"RustSpark exited with {status}")
            finally:
                finished.set()
                if watchdog is not None:
                    watchdog.cancel()
                if process.poll() is None:
                    stop_child()
                if process.stdout is not None:
                    process.stdout.close()
                process.wait()

    def query_one(self, sql: str | None, **options: Any) -> dict[str, Any] | None:
        """Return at most one row. Raise if the result contains multiple rows."""
        if "max_rows" in options:
            raise ValueError("query_one controls max_rows to detect duplicate rows")
        rows = self.query(sql, max_rows=2, **options)
        if len(rows) > 1:
            raise RustSparkError("query_one expected at most one row")
        return rows[0] if rows else None

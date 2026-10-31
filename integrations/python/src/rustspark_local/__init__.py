"""Local subprocess client for RustSpark, without SQL text interpolation."""

from __future__ import annotations

import json
import math
import os
import re
import subprocess

from .subprocess_budget import run_bounded
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

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


@dataclass(frozen=True)
class RustSpark:
    binary: str | os.PathLike[str] = "rustspark"
    database_dir: str | os.PathLike[str] | None = None

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
        if sql_file is None:
            if not isinstance(sql, str) or not sql.strip():
                raise ValueError("SQL cannot be blank")
        else:
            if not os.fspath(sql_file) or (sql is not None and
                (not isinstance(sql, str) or sql.strip())):
                raise ValueError("Provide exactly one inline SQL statement or sql_file")
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

        command = [os.fspath(self.binary)]
        if sql_file is None:
            command.extend(["--sql", sql])
        else:
            command.extend(["--sql-file", os.fspath(sql_file)])
        command.extend(["--stdout-format", "json-array"])
        if self.database_dir is not None:
            command.extend(["--database-dir", os.fspath(self.database_dir)])
        for flag, value in unnamed.items():
            if value is not None:
                command.extend([f"--{flag}", os.fspath(value)])
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
                or ":" not in typed_value or len(typed_value) > 8192):
                raise ValueError(f"Use bounded type:value for parameter {name!r}")
            command.extend(["--named-param", f"{name}={typed_value}"])
        if max_rows is not None:
            command.extend(["--max-rows", str(max_rows)])
        if query_timeout is not None:
            command.extend(["--query-timeout", str(query_timeout)])

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
        except (json.JSONDecodeError, ValueError) as exc:
            raise RustSparkError("RustSpark returned invalid JSON") from exc
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            raise RustSparkError("RustSpark returned an unexpected JSON result shape")
        return rows

    def query_one(self, sql: str | None, **options: Any) -> dict[str, Any] | None:
        """Return at most one row. Raise if the result contains multiple rows."""
        if "max_rows" in options:
            raise ValueError("query_one controls max_rows to detect duplicate rows")
        rows = self.query(sql, max_rows=2, **options)
        if len(rows) > 1:
            raise RustSparkError("query_one expected at most one row")
        return rows[0] if rows else None

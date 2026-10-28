"""Local subprocess client for RustSpark, without SQL text interpolation."""

from __future__ import annotations

import json
import math
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

__all__ = ["RustSpark", "RustSparkError"]


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
        sql: str,
        *,
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
    ) -> list[dict[str, Any]]:
        """Execute SQL and return JSON rows. Use max_rows to bound returned data.

        Named parameters are typed CLI values, e.g. {"min": "int64:10"}.
        The CLI binds them without substituting text into the SQL statement.
        """
        if not sql.strip():
            raise ValueError("SQL cannot be blank")
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

        command = [os.fspath(self.binary), "--sql", sql, "--stdout-format", "json-array"]
        if self.database_dir is not None:
            command.extend(["--database-dir", os.fspath(self.database_dir)])
        for flag, value in unnamed.items():
            if value is not None:
                command.extend([f"--{flag}", os.fspath(value)])
        for flag, values in named.items():
            for table, path in (values or {}).items():
                if not table or "=" in table:
                    raise ValueError(f"Invalid table name: {table!r}")
                command.extend([f"--{flag}", f"{table}={os.fspath(path)}"])
        for name, typed_value in (named_params or {}).items():
            if not name or not name.replace("_", "a").isalnum() or name[0].isdigit():
                raise ValueError(f"Invalid SQL parameter name: {name!r}")
            if ":" not in typed_value:
                raise ValueError(f"Use type:value for parameter {name!r}")
            command.extend(["--named-param", f"{name}={typed_value}"])
        if max_rows is not None:
            command.extend(["--max-rows", str(max_rows)])
        if query_timeout is not None:
            command.extend(["--query-timeout", str(query_timeout)])

        try:
            completed = subprocess.run(
                command, check=False, capture_output=True, text=True,
                encoding="utf-8", timeout=process_timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise RustSparkError(f"RustSpark exceeded process timeout: {process_timeout}s") from exc
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

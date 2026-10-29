"""Run local RustSpark SQL and return a pandas DataFrame using Arrow IPC."""
from __future__ import annotations

from pathlib import Path
import re
import subprocess
import tempfile
from typing import Mapping


def build_command(sql: str, output: Path, *, binary: str = "rustspark",
                  csv: str | None = None, parquet: str | None = None,
                  max_rows: int = 100_000, query_timeout: int | None = None,
                  named_params: Mapping[str, str] | None = None) -> list[str]:
    if not sql.strip():
        raise ValueError("SQL cannot be empty")
    if csv is not None and parquet is not None:
        raise ValueError("Specify CSV or Parquet, not both")
    if max_rows < 1:
        raise ValueError("max_rows must be positive")
    command = [
        binary, "--sql", sql, "--output-arrow", str(output),
        "--max-rows", str(max_rows),
    ]
    if csv is not None:
        command.extend(["--csv", csv])
    if parquet is not None:
        command.extend(["--parquet", parquet])
    if query_timeout is not None:
        if query_timeout <= 0:
            raise ValueError("query_timeout must be positive")
        command.extend(["--query-timeout", str(query_timeout)])
    if len(named_params or {}) > 64:
        raise ValueError("Too many SQL parameters")
    for name, value in sorted((named_params or {}).items()):
        if (not isinstance(name, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) is None
                or not isinstance(value, str) or ":" not in value or len(value) > 8192):
            raise ValueError("Parameters must be typed values with valid SQL names")
        command.extend(["--named-param", f"{name}={value}"])
    return command


def query_dataframe(sql: str, *, binary: str = "rustspark",
                    csv: str | None = None, parquet: str | None = None,
                    max_rows: int = 100_000, query_timeout: int | None = None,
                    named_params: Mapping[str, str] | None = None):
    """Load a bounded SQL result as pandas, preserving Arrow logical types.

    A DataFrame resides fully in Python memory. Prefer streaming for large data.
    """
    import pyarrow as pa

    with tempfile.TemporaryDirectory(prefix="rustspark-pandas-") as directory:
        output = Path(directory) / "result.arrow"
        command = build_command(
            sql, output, binary=binary, csv=csv, parquet=parquet,
            max_rows=max_rows, query_timeout=query_timeout,
            named_params=named_params,
        )
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        if result.returncode:
            raise RuntimeError(result.stderr.strip() or "RustSpark query failed")
        with pa.memory_map(str(output), "r") as source:
            return pa.ipc.open_file(source).read_all().to_pandas()

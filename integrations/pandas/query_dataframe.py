"""Run local RustSpark SQL and return a pandas DataFrame using Arrow IPC."""
from __future__ import annotations

from pathlib import Path
import re
import subprocess
import tempfile


TABLE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def append_named_tables(command, flag, tables):
    for name, path in sorted((tables or {}).items()):
        if not isinstance(name, str) or TABLE_NAME.fullmatch(name) is None:
            raise ValueError(f"Invalid SQL table name: {name!r}")
        if not isinstance(path, (str, Path)) or not str(path):
            raise ValueError("Named file paths must not be empty")
        command.extend([flag, f"{name}={path}"])


def build_command(sql: str, output: Path, *, binary: str = "rustspark",
                  csv: str | None = None, parquet: str | None = None,
                  max_rows: int = 100_000, query_timeout: int | None = None,
                  csv_tables: dict[str, str | Path] | None = None,
                  parquet_tables: dict[str, str | Path] | None = None) -> list[str]:
    if not sql.strip():
        raise ValueError("SQL cannot be empty")
    if (csv is not None or parquet is not None) and (csv_tables or parquet_tables):
        raise ValueError("Named and unnamed file inputs cannot be mixed")
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
    append_named_tables(command, "--csv-table", csv_tables)
    append_named_tables(command, "--parquet-table", parquet_tables)
    if query_timeout is not None:
        if query_timeout <= 0:
            raise ValueError("query_timeout must be positive")
        command.extend(["--query-timeout", str(query_timeout)])
    return command


def query_dataframe(sql: str, *, binary: str = "rustspark",
                    csv: str | None = None, parquet: str | None = None,
                    max_rows: int = 100_000, query_timeout: int | None = None,
                    csv_tables: dict[str, str | Path] | None = None,
                    parquet_tables: dict[str, str | Path] | None = None):
    """Load a bounded SQL result as pandas, preserving Arrow logical types.

    A DataFrame resides fully in Python memory. Prefer streaming for large data.
    """
    import pyarrow as pa

    with tempfile.TemporaryDirectory(prefix="rustspark-pandas-") as directory:
        output = Path(directory) / "result.arrow"
        command = build_command(
            sql, output, binary=binary, csv=csv, parquet=parquet,
            max_rows=max_rows, query_timeout=query_timeout,
            csv_tables=csv_tables, parquet_tables=parquet_tables,
        )
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        if result.returncode:
            raise RuntimeError(result.stderr.strip() or "RustSpark query failed")
        with pa.memory_map(str(output), "r") as source:
            return pa.ipc.open_file(source).read_all().to_pandas()

"""Produce a Polars LazyFrame from a RustSpark Arrow IPC file."""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import math
import subprocess
import tempfile
from typing import Iterator


def build_command(sql: str, output: Path, *, binary: str = "rustspark",
                  csv: str | None = None, parquet: str | None = None,
                  query_timeout: int | None = None) -> list[str]:
    if not sql.strip():
        raise ValueError("SQL cannot be blank")
    if csv is not None and parquet is not None:
        raise ValueError("Use only one file source")
    command = [binary, "--sql", sql, "--output-arrow", str(output)]
    if csv is not None:
        command.extend(["--csv", csv])
    if parquet is not None:
        command.extend(["--parquet", parquet])
    if query_timeout is not None:
        if query_timeout <= 0:
            raise ValueError("query_timeout must be positive")
        command.extend(["--query-timeout", str(query_timeout)])
    return command


@contextmanager
def rustspark_lazy_frame(sql: str, *, binary: str = "rustspark",
                         csv: str | None = None, parquet: str | None = None,
                         query_timeout: int | None = None,
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
            query_timeout=query_timeout,
        )
        try:
            completed = subprocess.run(command, capture_output=True, text=True,
                                       check=False, timeout=process_timeout)
        except subprocess.TimeoutExpired as error:
            raise RuntimeError("RustSpark Arrow export exceeded process timeout") from error
        if completed.returncode:
            raise RuntimeError(completed.stderr.strip() or "RustSpark query failed")
        yield pl.scan_ipc(result)

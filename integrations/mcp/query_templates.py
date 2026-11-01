"""Allowlisted SQL template handling shared by MCP tools (stdlib only)."""
from __future__ import annotations

from pathlib import Path
import math
import json
import re
import os
import stat
import tempfile
from contextlib import contextmanager
from typing import Iterator

TEMPLATE_NAME = re.compile(r"[a-zA-Z_][a-zA-Z0-9_-]{0,63}\Z")
PARAM_NAME = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*\Z")


def list_templates(directory: Path) -> list[str]:
    if not directory.is_dir():
        raise ValueError("RUSTSPARK_MCP_QUERIES_DIR must exist")
    return sorted(
        path.stem for path in directory.iterdir()
        if path.is_file() and not path.is_symlink()
        and path.suffix == ".sql" and TEMPLATE_NAME.fullmatch(path.stem)
    )


def template_path(directory: Path, name: str) -> Path:
    if not isinstance(name, str) or not TEMPLATE_NAME.fullmatch(name):
        raise ValueError("Invalid template name")
    path = directory / f"{name}.sql"
    if not path.is_file() or path.is_symlink():
        raise ValueError("Unknown query template")
    if path.resolve().parent != directory.resolve():
        raise ValueError("Template cannot escape configured directory")
    return path


MAX_TEMPLATE_BYTES = 1024 * 1024


@contextmanager
def pinned_template(path: Path) -> Iterator[Path]:
    """Use an immutable private copy after safely opening a reviewed SQL file.

    O_NOFOLLOW pins the inspected inode instead of rechecking an attacker-
    replaceable path just before the backend opens it.
    """
    flags = os.O_RDONLY
    # A replaced FIFO must not block a worker before fstat can reject it.
    if hasattr(os, "O_NONBLOCK"):
        flags |= os.O_NONBLOCK
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    # On platforms without O_NOFOLLOW, reject symlinks at the last check.
    if not hasattr(os, "O_NOFOLLOW") and path.is_symlink():
        raise ValueError("SQL template cannot be a symlink")
    try:
        fd = os.open(path, flags)
    except OSError as error:
        raise ValueError("Unable to open approved SQL template") from error
    with os.fdopen(fd, "rb") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_TEMPLATE_BYTES:
            raise ValueError("SQL template must be a bounded regular file")
        sql = source.read(MAX_TEMPLATE_BYTES + 1)
        if len(sql) > MAX_TEMPLATE_BYTES:
            raise ValueError("SQL template exceeds byte limit")
    with tempfile.TemporaryDirectory(prefix="rustspark-mcp-sql-") as directory:
        snapshot = Path(directory) / "query.sql"
        snapshot.write_bytes(sql)
        snapshot.chmod(0o600)
        yield snapshot


def make_command(binary: str, database_dir: Path, template: Path,
                 parameters: dict[str, str], max_rows: int = 200,
                 query_timeout: int = 20) -> list[str]:
    if len(parameters) > 32:
        raise ValueError("Too many SQL parameters")
    if type(max_rows) is not int or not 1 <= max_rows <= 200:
        raise ValueError("max_rows must be between 1 and 200")
    if type(query_timeout) is not int or not 1 <= query_timeout <= 20:
        raise ValueError("query_timeout must be between 1 and 20 seconds")
    arguments = [
        binary, "--database-dir", str(database_dir), "--sql-file", str(template),
        "--stdout-format", "json-array", "--max-rows", str(max_rows),
        "--query-timeout", str(query_timeout),
    ]
    if any(not isinstance(key, str) or not PARAM_NAME.fullmatch(key) for key in parameters):
        raise ValueError("Invalid SQL parameter name")
    for key, value in sorted(parameters.items()):
        if not isinstance(value, str) or ":" not in value or len(value.encode("utf-8")) > 4096:
            raise ValueError("Parameters must be typed values, e.g. int64:42")
        arguments.extend(["--named-param", f"{key}={value}"])
    return arguments


def parse_query_rows(text: str) -> list[dict]:
    def reject_nonfinite(value):
        raise ValueError(f"Nonfinite SQL JSON constant: {value}")

    def parse_finite_float(value):
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("Nonfinite SQL JSON number")
        return parsed

    def unique_fields(pairs):
        fields = {}
        for key, value in pairs:
            if key in fields:
                raise ValueError(f"Duplicate SQL result key: {key}")
            fields[key] = value
        return fields

    try:
        rows = json.loads(text, object_pairs_hook=unique_fields,
                          parse_constant=reject_nonfinite, parse_float=parse_finite_float)
    except RecursionError as error:
        raise ValueError("SQL result exceeds JSON nesting limit") from error
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError("SQL template results must be an array of row objects")
    return rows

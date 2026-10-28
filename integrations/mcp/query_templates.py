"""Allowlisted SQL template handling shared by MCP tools (stdlib only)."""
from __future__ import annotations

from pathlib import Path
import re

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
    if not TEMPLATE_NAME.fullmatch(name):
        raise ValueError("Invalid template name")
    path = directory / f"{name}.sql"
    if not path.is_file() or path.is_symlink():
        raise ValueError("Unknown query template")
    if path.resolve().parent != directory.resolve():
        raise ValueError("Template cannot escape configured directory")
    return path


def make_command(binary: str, database_dir: Path, template: Path,
                 parameters: dict[str, str], max_rows: int = 200,
                 query_timeout: int = 20) -> list[str]:
    if len(parameters) > 32:
        raise ValueError("Too many SQL parameters")
    if max_rows < 1 or query_timeout < 1:
        raise ValueError("Query deadline and row limit must be positive")
    arguments = [
        binary, "--database-dir", str(database_dir), "--sql-file", str(template),
        "--stdout-format", "json-array", "--max-rows", str(max_rows),
        "--query-timeout", str(query_timeout),
    ]
    for key, value in sorted(parameters.items()):
        if not PARAM_NAME.fullmatch(key) or not isinstance(value, str) or ":" not in value or len(value) > 4096:
            raise ValueError("Parameters must be typed values, e.g. int64:42")
        arguments.extend(["--named-param", f"{key}={value}"])
    return arguments

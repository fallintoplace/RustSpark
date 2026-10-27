#!/usr/bin/env python3
"""Fetch and verify the official ClickBench DataFusion query corpus."""

import argparse
import hashlib
from pathlib import Path
import tempfile
from urllib.request import urlopen


UPSTREAM_COMMIT = "36250a94d5ae6ce03df29b876c99478608119116"
UPSTREAM_BLOB_SHA1 = "5be85b3f875c2c375e078308dde3edf678d0008c"
UPSTREAM_URL = (
    "https://raw.githubusercontent.com/ClickHouse/ClickBench/"
    f"{UPSTREAM_COMMIT}/datafusion/queries.sql"
)
QUERY_COUNT = 43
MAX_BYTES = 65536


def git_blob_sha1(data: bytes) -> str:
    prefix = f"blob {len(data)}".encode("ascii") + bytes([0])
    return hashlib.sha1(prefix + data).hexdigest()


def verify_queries(data: bytes) -> None:
    if len(data) > MAX_BYTES:
        raise ValueError("ClickBench corpus exceeds the size limit")
    if git_blob_sha1(data) != UPSTREAM_BLOB_SHA1:
        raise ValueError("ClickBench query corpus hash does not match the pinned upstream blob")
    lines = [line.strip() for line in data.decode("utf-8").splitlines() if line.strip()]
    if len(lines) != QUERY_COUNT:
        raise ValueError(f"Expected {QUERY_COUNT} queries, found {len(lines)}")
    if any(not line.upper().startswith("SELECT ") or not line.endswith(";") for line in lines):
        raise ValueError("ClickBench corpus must contain one SELECT statement per line")


def fetch_queries() -> bytes:
    with urlopen(UPSTREAM_URL, timeout=30) as response:
        data = response.read(MAX_BYTES + 1)
    verify_queries(data)
    return data


def write_queries(path: Path, data: bytes, overwrite: bool = False) -> None:
    if (path.exists() or path.is_symlink()) and not overwrite:
        raise FileExistsError(f"Refusing to replace existing file: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=path.parent, prefix=".clickbench-", delete=False
        ) as stream:
            temporary = Path(stream.name)
            stream.write(data)
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args(argv)
    try:
        data = fetch_queries()
        write_queries(args.output, data, overwrite=args.overwrite)
    except (OSError, ValueError, UnicodeError) as error:
        parser.exit(1, f"ClickBench corpus fetch failed: {error}\n")
    print(f"Verified {QUERY_COUNT} official ClickBench queries: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Validate a tpch_prepare.py fixture and its 22 pinned query hashes."""

import argparse
import hashlib
import json
import math
from pathlib import Path

from tpch_prepare import sha256_file
from tpch_run import unique_keys

TABLES = frozenset(("customer", "lineitem", "nation", "orders",
                    "part", "partsupp", "region", "supplier"))


def inside(root, relative):
    if not isinstance(relative, str) or not relative:
        raise ValueError("Manifest path must be a nonempty string")
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError(f"Manifest path escapes fixture root: {relative}")
    if not path.is_file():
        raise ValueError(f"Missing fixture file: {relative}")
    return path


def check_table(root, manifest, hashes, table):
    path = f"data/{table}.parquet"
    if manifest["tables"][table] != path:
        raise ValueError(f"Unexpected Parquet path for {table}")
    actual = sha256_file(inside(root, path))
    expected = hashes[table]
    if not isinstance(expected, str) or actual != expected:
        raise ValueError(f"Parquet checksum mismatch for {table}")
    count = manifest["rows"][table]
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise ValueError(f"Invalid row count for {table}")


def check_queries(root, queries):
    if not isinstance(queries, list) or len(queries) != 22:
        raise ValueError("Expected exactly 22 TPC-H queries")
    numbers = set()
    for query in queries:
        if not isinstance(query, dict):
            raise ValueError("Invalid SQL manifest entry")
        number = query.get("id")
        if isinstance(number, bool) or not isinstance(number, int) or number not in range(1, 23) or number in numbers:
            raise ValueError("Invalid or duplicate TPC-H query ID")
        numbers.add(number)
        relative = f"queries/{number:02}.sql"
        if query.get("path") != relative:
            raise ValueError(f"Wrong SQL path for query {number}")
        sql = inside(root, relative).read_bytes()
        if not sql.strip() or hashlib.sha256(sql).hexdigest() != query.get("sha256"):
            raise ValueError(f"SQL content/hash mismatch for query {number}")
    if numbers != set(range(1, 23)):
        raise ValueError("TPC-H query IDs are incomplete")
    return len(numbers)


def require_table_mapping(manifest, key, message):
    """Require exactly the expected TPC-H table keys."""
    mapping = manifest.get(key)
    if not isinstance(mapping, dict) or set(mapping) != TABLES:
        raise ValueError(message)
    return mapping


def check(root):
    root = Path(root)
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"),
                          object_pairs_hook=unique_keys)
    if not isinstance(manifest, dict):
        raise ValueError("TPC-H fixture manifest must be a JSON object")
    scale = manifest.get("scale_factor")
    if isinstance(scale, bool) or not isinstance(scale, (float, int)) or not math.isfinite(scale) or scale <= 0:
        raise ValueError("Invalid TPC-H scale factor")
    require_table_mapping(manifest, "tables", "Expected exactly eight TPC-H tables")
    hashes = require_table_mapping(manifest, "table_sha256", "Missing TPC-H Parquet checksums")
    require_table_mapping(manifest, "rows", "Missing TPC-H row counts")
    for table in sorted(TABLES):
        check_table(root, manifest, hashes, table)
    query_count = check_queries(root, manifest.get("queries"))
    return {"scale_factor": scale, "tables": len(TABLES), "queries": query_count}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path)
    args = parser.parse_args(argv)
    try:
        print(json.dumps(check(args.fixture), indent=2))
    except (OSError, ValueError, TypeError) as error:
        parser.exit(1, f"invalid TPC-H fixture: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

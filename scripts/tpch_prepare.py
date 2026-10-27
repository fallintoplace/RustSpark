#!/usr/bin/env python3
"""Generate local analytical SQL and Parquet fixtures using DuckDB."""

import argparse
import hashlib
import json
import math
from pathlib import Path

TABLES = ("customer", "lineitem", "nation", "orders",
          "part", "partsupp", "region", "supplier")


def prepare(connection, destination, scale):
    if not math.isfinite(scale) or scale <= 0:
        raise ValueError("Scale factor must be a finite positive number")
    output = Path(destination)
    if output.exists() and any(output.iterdir()):
        raise ValueError(f"Destination must be empty: {output}")
    parquet_dir = output / "data"
    sql_dir = output / "queries"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    sql_dir.mkdir(parents=True, exist_ok=True)
    connection.execute(f"CALL dbgen(sf={scale:g})")
    counts = {}
    for table in TABLES:
        parquet_path = parquet_dir / f"{table}.parquet"
        escaped = str(parquet_path).replace("'", "''")
        connection.execute(
            f"COPY (SELECT * FROM {table}) TO '{escaped}' (FORMAT PARQUET)"
        )
        counts[table] = connection.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]
    queries = connection.execute(
        "SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr"
    ).fetchall()
    if len(queries) != 22 or [n for n, _ in queries] != list(range(1, 23)):
        raise ValueError("Expected exactly 22 numbered SQL queries")
    entries = []
    for number, sql in queries:
        path = sql_dir / f"{number:02}.sql"
        path.write_text(sql.rstrip() + "\n", encoding="utf-8")
        entries.append({
            "id": number,
            "path": f"queries/{number:02}.sql",
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        })
    manifest = {
        "scale_factor": scale,
        "tables": {table: f"data/{table}.parquet" for table in TABLES},
        "rows": counts,
        "queries": entries,
    }
    (output / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scale-factor", type=float, default=0.1)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if not math.isfinite(args.scale_factor) or args.scale_factor <= 0:
            raise ValueError("Scale factor must be a finite positive number")
        import duckdb
        conn = duckdb.connect(":memory:")
        try:
            conn.execute("INSTALL tpch")
            conn.execute("LOAD tpch")
            result = prepare(conn, args.output, args.scale_factor)
        finally:
            conn.close()
    except (ImportError, OSError, ValueError, RuntimeError) as error:
        parser.exit(1, f"error: {error}\n")
    print(f"Wrote {len(result['tables'])} Parquet tables and "
          f"{len(result['queries'])} SQL queries to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

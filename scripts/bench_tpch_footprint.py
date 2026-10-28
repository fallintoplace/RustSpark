#!/usr/bin/env python3
"""Audit exact TPC-H Parquet input bytes before comparing query speed."""

import argparse
import hashlib
import json
from pathlib import Path

from bench_tpch_manifest import TABLES, check, inside


def streamed_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def profile(fixture):
    fixture = Path(fixture)
    check(fixture)
    manifest = json.loads((fixture / "manifest.json").read_text(encoding="utf-8"))
    tables = {}
    for table in sorted(TABLES):
        path = inside(fixture, manifest["tables"][table])
        rows = manifest["rows"][table]
        size = path.stat().st_size
        tables[table] = {
            "rows": rows, "physical_bytes": size, "sha256": streamed_sha256(path),
            "physical_bytes_per_row": round(size / rows, 3) if rows else None,
        }
    return {
        "metric": "physical Parquet fixture size and exact file identity",
        "warning": "Disk bytes are not scan bytes or decompressed data size",
        "scale_factor": manifest["scale_factor"],
        "total_physical_bytes": sum(item["physical_bytes"] for item in tables.values()),
        "tables": tables,
        "query_sha256": {entry["id"]: entry["sha256"] for entry in manifest["queries"]},
    }


def compare(a, b):
    if a["scale_factor"] != b["scale_factor"]:
        raise ValueError("TPC-H scale factors differ")
    changes = []
    for table in sorted(TABLES):
        if (a["tables"][table]["sha256"] != b["tables"][table]["sha256"]
                or a["tables"][table]["rows"] != b["tables"][table]["rows"]):
            changes.append(table)
    changed_sql = [number for number in range(1, 23)
                   if a["query_sha256"][number] != b["query_sha256"][number]]
    return {"identical_parquet_inputs": not changes,
            "changed_tables": changes,
            "identical_sql": not changed_sql,
            "changed_queries": changed_sql}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path)
    parser.add_argument("--compare-to", type=Path, help="Require identical SQL and Parquet bytes")
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args(argv)
    try:
        output = profile(args.fixture)
        result = None
        if args.compare_to:
            result = compare(output, profile(args.compare_to))
            output["comparison"] = result
        rendered = json.dumps(output, indent=2) + "\n"
        if args.json_out:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(rendered, encoding="utf-8")
        print(rendered, end="")
    except (OSError, TypeError, ValueError, KeyError) as error:
        parser.exit(1, f"TPC-H input audit failed: {error}\n")
    return 1 if result and not (result["identical_parquet_inputs"] and result["identical_sql"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())

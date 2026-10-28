#!/usr/bin/env python3
"""Benchmark full scans and aggregations on a billion-row TPC-H lineitem table.

Prepare data with tpch_prepare.py --scale-factor 167 (or larger). This script
does not generate a giant fixture or run on normal PR CI.
"""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import tempfile

from tpch_run import command_for, input_arguments, measure

QUERIES = (
    ("scan_sum", "SELECT SUM(l_quantity) AS total FROM lineitem", False),
    ("selective_sum",
     "SELECT SUM(l_extendedprice) AS revenue FROM lineitem "
     "WHERE l_shipdate >= DATE '1998-01-01'", False),
    ("grouped_sum",
     "SELECT l_returnflag, SUM(l_extendedprice) AS revenue "
     "FROM lineitem GROUP BY l_returnflag ORDER BY l_returnflag", True),
)


def validate_fixture(fixture, minimum_rows):
    """Reject smaller or incomplete inputs rather than labeling them billion-row."""
    if type(minimum_rows) is not int or minimum_rows < 1:
        raise ValueError("minimum rows must be a positive integer")
    fixture = Path(fixture)
    manifest = json.loads((fixture / "manifest.json").read_text(encoding="utf-8"))
    rows = manifest.get("rows", {}).get("lineitem")
    if type(rows) is not int or rows < minimum_rows:
        raise ValueError(
            f"TPC-H lineitem must contain >= {minimum_rows:,} rows according to "
            f"manifest.json; found {rows!r}"
        )
    inputs = input_arguments(fixture / "data")
    return rows, inputs


def benchmark(binary, fixture, minimum_rows, repeat, warmup, timeout,
              partitions, batch_size=None, pushdown_filters=False):
    if (any(type(value) is not int or value < 1
            for value in (repeat, timeout, partitions))
            or type(warmup) is not int or warmup < 0):
        raise ValueError("repeat, timeout, partitions must be positive; warmup >= 0")
    if batch_size is not None and (type(batch_size) is not int or batch_size < 1):
        raise ValueError("batch size must be positive")
    rows, inputs = validate_fixture(fixture, minimum_rows)
    results = []
    with tempfile.TemporaryDirectory(prefix="rustspark-billion-bench-") as directory:
        for index, (name, sql, ordered) in enumerate(QUERIES, 1):
            query_file = Path(directory) / f"{index:02}.sql"
            query_file.write_text(sql + "\n", encoding="utf-8")
            command = command_for(
                binary, inputs, query_file, partitions,
                batch_size=batch_size, pushdown_filters=pushdown_filters,
            )
            result = {"name": name, "sql": sql, "ordered": ordered}
            try:
                result.update(measure(command, repeat, warmup, timeout, ordered=ordered))
            except (OSError, RuntimeError, ValueError) as error:
                result.update({"status": "error", "error": str(error)[-900:]})
            results.append(result)
    return {
        "metric": "cold local CLI + Parquet registration + SQL + JSON output",
        "warning": "Manifest row count is trusted, not independently counted. "
                   "SUM/GROUP BY require reading column data; no subsecond guarantee.",
        "rows_in_lineitem": rows,
        "minimum_rows": minimum_rows,
        "fixture": str(Path(fixture).resolve()),
        "binary": str(binary),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "target_partitions": partitions,
        "batch_size": batch_size,
        "pushdown_filters": pushdown_filters,
        "repeat": repeat,
        "warmup": warmup,
        "results": results,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", required=True, type=Path)
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--minimum-rows", type=int, default=1_000_000_000)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--target-partitions", type=int, default=8)
    parser.add_argument("--batch-size", type=int)
    parser.add_argument("--parquet-pushdown-filters", action="store_true")
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args(argv)
    try:
        report = benchmark(
            args.binary, args.fixture, args.minimum_rows, args.repeat,
            args.warmup, args.timeout, args.target_partitions,
            args.batch_size, args.parquet_pushdown_filters,
        )
        rendered = json.dumps(report, indent=2) + "\n"
        if args.json_out:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(rendered, encoding="utf-8")
        print(rendered, end="")
    except (OSError, ValueError, json.JSONDecodeError) as error:
        parser.exit(1, f"Billion-row benchmark failed: {error}\n")
    return 1 if any(item["status"] != "ok" for item in report["results"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())

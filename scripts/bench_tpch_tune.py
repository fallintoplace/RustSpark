#!/usr/bin/env python3
"""Sweep TPC-H execution settings and reject configurations with different results."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import re

from clickbench_run import has_top_level_order_by
from tpch_run import command_for, input_arguments, measure, query_paths

VALUE_FLAGS = {"--batch-size", "--memory-limit-mb", "--scan-split-min-kb",
               "--metadata-fetch-concurrency"}
BOOLEAN_FLAGS = {"--parquet-pushdown-filters", "--parquet-reorder-filters"}


def parse_variants(raw):
    if not isinstance(raw, list) or not 2 <= len(raw) <= 20:
        raise ValueError("Expected 2 to 20 variants")
    names = set()
    result = []
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("Each variant must be an object")
        name, parts, flags = item.get("name"), item.get("partitions"), item.get("flags", [])
        if not isinstance(name, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,39}", name):
            raise ValueError("Invalid variant name")
        if name in names:
            raise ValueError("Duplicate variant name")
        if type(parts) is not int or parts < 1:
            raise ValueError("Partitions must be a positive integer")
        if not isinstance(flags, list) or not all(type(x) is str for x in flags):
            raise ValueError("Variant flags must be a list of strings")
        normalized, seen = [], set()
        index = 0
        while index < len(flags):
            flag = flags[index]
            if flag in seen or flag not in VALUE_FLAGS | BOOLEAN_FLAGS:
                raise ValueError(f"Unsupported or duplicate flag: {flag}")
            seen.add(flag)
            normalized.append(flag)
            if flag in VALUE_FLAGS:
                if (index + 1 >= len(flags) or not flags[index + 1].isascii()
                        or not flags[index + 1].isdecimal()
                        or int(flags[index + 1]) < 1):
                    raise ValueError(f"{flag} requires a positive integer")
                normalized.append(flags[index + 1])
                index += 1
            index += 1
        if "--parquet-reorder-filters" in seen and "--parquet-pushdown-filters" not in seen:
            raise ValueError("Parquet filter reordering requires pushdown")
        result.append({"name": name, "partitions": parts, "flags": normalized})
        names.add(name)
    return result


def run(binary, parquet, queries, variants, repeat=3, warmup=1, timeout=180):
    if type(repeat) is not int or repeat < 1 or type(warmup) is not int or warmup < 0 or timeout < 1:
        raise ValueError("Invalid timing settings")
    inputs = input_arguments(parquet)
    expected = {}
    configurations = []
    for variant in variants:
        results = []
        for number, file in queries:
            sql = file.read_text(encoding="utf-8")
            command = command_for(binary, inputs, file, variant["partitions"]) + variant["flags"]
            entry = measure(command, repeat, warmup, timeout,
                            ordered=has_top_level_order_by(sql))
            if entry["status"] != "ok":
                raise ValueError(f"Q{number:02} failed for {variant['name']}")
            digest = entry.get("result_sha256")
            median = entry.get("median_ms")
            if type(digest) is not str or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                raise ValueError(f"Q{number:02} invalid result fingerprint")
            if (type(median) not in (int, float) or not math.isfinite(median)
                    or median <= 0):
                raise ValueError(f"Q{number:02} invalid median timing")
            if number in expected and expected[number] != digest:
                raise ValueError(f"Q{number:02} result changed for {variant['name']}")
            expected[number] = digest
            results.append({"id": number, "sql_sha256": hashlib.sha256(file.read_bytes()).hexdigest(),
                            **entry})
        configurations.append({**variant, "results": results,
                               "total_median_ms": round(math.fsum(x["median_ms"] for x in results), 3)})
    complete = len(queries) == 22 and {i for i, _ in queries} == set(range(1, 23))
    best = min(configurations, key=lambda row: row["total_median_ms"]) if complete else None
    return {"metric": "sum of per-query cold CLI median times, not TPC throughput",
            "complete_22_queries": complete,
            "best_complete_variant": best["name"] if best else None,
            "warmup": warmup, "repeat": repeat, "configurations": configurations}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("variants", type=Path, help="JSON array of named execution configurations")
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--parquet-dir", type=Path, required=True)
    parser.add_argument("--queries-dir", type=Path, required=True)
    parser.add_argument("--query-ids", nargs="+", type=int)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args(argv)
    try:
        variants = parse_variants(json.loads(args.variants.read_text(encoding="utf-8")))
        result = run(args.binary, args.parquet_dir, query_paths(args.queries_dir, args.query_ids),
                     variants, args.repeat, args.warmup, args.timeout)
        rendered = json.dumps(result, indent=2) + "\n"
        if args.json_out:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(rendered, encoding="utf-8")
        print(rendered, end="")
    except (OSError, ValueError, RuntimeError) as error:
        parser.exit(1, f"TPC-H tuning failed: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

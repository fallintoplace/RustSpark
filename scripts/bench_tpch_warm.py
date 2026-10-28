#!/usr/bin/env python3
"""Measure TPC-H SQL in one warmed RustSpark shell, excluding process startup."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import statistics
import subprocess

from clickbench_run import has_top_level_order_by
from tpch_run import (input_arguments, query_paths, result_signature,
                      reject_nonfinite, unique_keys)

TIMER = re.compile(r"^Time:\s+(\d+(?:\.\d+)?)\s+ms$", re.MULTILINE)
BOUNDARY = "__rustspark_tpch_boundary"


def requests_for(queries, warmup, repeat):
    if warmup < 0 or repeat < 1 or not queries:
        raise ValueError("Queries required; warmup >= 0 and repeat >= 1")
    ids = [number for number, _ in queries]
    if (any(type(number) is not int or number not in range(1, 23) for number in ids)
            or len(set(ids)) != len(ids)):
        raise ValueError("Invalid or duplicate TPC-H query IDs")
    requests = []
    for number, path in queries:
        sql_bytes = path.read_bytes()
        sql = sql_bytes.decode("utf-8").strip()
        if not sql:
            raise ValueError(f"Empty SQL file: {path}")
        for attempt in range(warmup + repeat):
            requests.append({"id": number, "sql": sql,
                             "sql_sha256": hashlib.sha256(sql_bytes).hexdigest(),
                             "attempt": attempt, "ordered": has_top_level_order_by(sql)})
    return requests


def shell_script(requests):
    statements = [".timer on"]
    for index, request in enumerate(requests):
        for phase in ("start", "end"):
            if phase == "start":
                statements.append(f"SELECT '{phase}:{index}' AS {BOUNDARY};")
                statements.append(request["sql"].rstrip(";") + ";")
            else:
                statements.append(f"SELECT '{phase}:{index}' AS {BOUNDARY};")
    statements.append(".quit")
    return "\n".join(statements) + "\n"


def parse_shell(stdout, stderr, requests, warmup, repeat):
    if "SQL error:" in stderr or "Shell error:" in stderr:
        raise RuntimeError("RustSpark interactive query failed: " + stderr[-900:])
    timers = [float(value) for value in TIMER.findall(stderr)]
    if len(timers) != 3 * len(requests):
        raise ValueError(f"Expected {3 * len(requests)} statement timers, got {len(timers)}")
    rows = []
    for line in stdout.splitlines():
        if line.strip():
            row = json.loads(line, parse_constant=reject_nonfinite,
                             object_pairs_hook=unique_keys)
            if not isinstance(row, dict):
                raise ValueError("Expected JSON objects in warm SQL output")
            rows.append(row)
    cursor = 0
    grouped = {}
    for index, request in enumerate(requests):
        if cursor >= len(rows) or rows[cursor] != {BOUNDARY: f"start:{index}"}:
            raise ValueError(f"Missing start boundary for statement {index}")
        cursor += 1
        values = []
        while cursor < len(rows) and rows[cursor] != {BOUNDARY: f"end:{index}"}:
            values.append(rows[cursor])
            cursor += 1
        if cursor >= len(rows):
            raise ValueError(f"Missing end boundary for statement {index}")
        cursor += 1
        digest, count = result_signature("\n".join(json.dumps(r) for r in values),
                                         ordered=request["ordered"])
        elapsed = timers[3 * index + 1]
        if not math.isfinite(elapsed) or elapsed < 0:
            raise ValueError("Invalid warm SQL statement time")
        group = grouped.setdefault(request["id"], {"result_sha256": digest,
                                                   "sql_sha256": request["sql_sha256"],
                                                   "result_rows": count, "samples_ms": []})
        if group["result_sha256"] != digest or group["result_rows"] != count:
            raise ValueError(f"Nondeterministic results for Q{request['id']:02}")
        if request["attempt"] >= warmup:
            group["samples_ms"].append(elapsed)
    if cursor != len(rows):
        raise ValueError("Unexpected rows after warm SQL boundaries")
    for number, result in grouped.items():
        samples = result["samples_ms"]
        if len(samples) != repeat:
            raise ValueError(f"Query Q{number:02} has missing samples")
        result["median_ms"] = round(statistics.median(samples), 3)
        result["p95_ms"] = sorted(samples)[math.ceil(0.95 * len(samples)) - 1]
        result["status"] = "ok"
    return [{"id": number, **row} for number, row in sorted(grouped.items())]


def benchmark(binary, queries, parquet, warmup=1, repeat=5, partitions=2, timeout=3600):
    if partitions < 1 or timeout < 1:
        raise ValueError("Positive partitions and timeout required")
    requests = requests_for(queries, warmup, repeat)
    command = [str(binary), *input_arguments(parquet), "--interactive",
               "--stdout-format", "json", "--target-partitions", str(partitions)]
    completed = subprocess.run(command, input=shell_script(requests), text=True,
                               capture_output=True, timeout=timeout, check=False)
    if completed.returncode:
        raise RuntimeError(f"RustSpark shell exited {completed.returncode}: {completed.stderr[-900:]}")
    results = parse_shell(completed.stdout, completed.stderr, requests, warmup, repeat)
    return {"metric": "warm interactive SQL planning, execution and JSON output",
            "warning": "Not TPC throughput; excludes process startup and input registration",
            "warmup": warmup, "repeat": repeat, "target_partitions": partitions,
            "results": results}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--queries-dir", type=Path, required=True)
    parser.add_argument("--parquet-dir", type=Path, required=True)
    parser.add_argument("--query-ids", type=int, nargs="+")
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--target-partitions", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args(argv)
    try:
        report = benchmark(args.binary, query_paths(args.queries_dir, args.query_ids),
                           args.parquet_dir, args.warmup, args.repeat,
                           args.target_partitions, args.timeout)
        output = json.dumps(report, indent=2) + "\n"
        if args.json_out:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(output, encoding="utf-8")
        print(output, end="")
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        parser.exit(1, f"TPC-H warm benchmark failed: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

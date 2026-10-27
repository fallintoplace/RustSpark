#!/usr/bin/env python3
"""Compare two RustSpark builds with interleaved cold-process TPC-H trials."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import platform
import statistics
import subprocess
import time

from clickbench_run import has_top_level_order_by
from tpch_run import command_for, input_arguments, query_paths, result_signature

ORDER = ("baseline", "candidate", "candidate", "baseline")


def timed_command(command, timeout, ordered):
    started = time.perf_counter()
    proc = subprocess.run(command, text=True, capture_output=True, timeout=timeout, check=False)
    elapsed = (time.perf_counter() - started) * 1000
    if proc.returncode:
        raise RuntimeError(f"SQL exited {proc.returncode}: {proc.stderr[-600:]}")
    fingerprint, rows = result_signature(proc.stdout, ordered=ordered)
    if not math.isfinite(elapsed) or elapsed <= 0:
        raise ValueError("Invalid cold-process query timing")
    return elapsed, fingerprint, rows


def compare_query(commands, ordered, repeat, warmup, timeout):
    if type(repeat) is not int or repeat < 1 or type(warmup) is not int or warmup < 0:
        raise ValueError("Repeat must be positive; warmup nonnegative")
    samples = {"baseline": [], "candidate": []}
    digest = None
    result_rows = None
    for cycle in range(repeat + warmup):
        for engine in ORDER:
            ms, current_digest, count = timed_command(commands[engine], timeout, ordered)
            if digest is not None and (current_digest != digest or count != result_rows):
                raise ValueError("Baseline and candidate produced different SQL results")
            digest, result_rows = current_digest, count
            if cycle >= warmup:
                samples[engine].append(ms)
    old = statistics.median(samples["baseline"])
    new = statistics.median(samples["candidate"])
    return {"status": "ok", "result_sha256": digest, "result_rows": result_rows,
            "baseline_samples_ms": [round(x, 3) for x in samples["baseline"]],
            "candidate_samples_ms": [round(x, 3) for x in samples["candidate"]],
            "baseline_median_ms": round(old, 3), "candidate_median_ms": round(new, 3),
            "baseline_over_candidate": round(old / new, 4)}


def benchmark(baseline, candidate, parquet, queries, repeat=3, warmup=1,
              timeout=180, partitions=2):
    if timeout < 1 or partitions < 1:
        raise ValueError("Timeout and partitions must be positive")
    inputs = input_arguments(parquet)
    results = []
    for number, sql_file in queries:
        sql = sql_file.read_text(encoding="utf-8")
        commands = {engine: command_for(binary, inputs, sql_file, partitions)
                    for engine, binary in (("baseline", baseline), ("candidate", candidate))}
        entry = {"id": number, "sql_sha256": hashlib.sha256(sql_file.read_bytes()).hexdigest()}
        try:
            entry.update(compare_query(commands, has_top_level_order_by(sql), repeat,
                                       warmup, timeout))
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
            entry.update({"status": "error", "error": str(error)[-600:]})
        results.append(entry)
    return {"metric": "ABBA interleaved cold-process end-to-end latency",
            "warning": "Not a TPC-certified performance score",
            "platform": platform.platform(), "repeat": repeat, "warmup": warmup,
            "target_partitions": partitions, "results": results}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--queries-dir", required=True, type=Path)
    parser.add_argument("--parquet-dir", required=True, type=Path)
    parser.add_argument("--query-ids", nargs="+", type=int)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--target-partitions", type=int, default=2)
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args(argv)
    try:
        report = benchmark(args.baseline, args.candidate, args.parquet_dir,
                           query_paths(args.queries_dir, args.query_ids),
                           args.repeat, args.warmup, args.timeout, args.target_partitions)
        output = json.dumps(report, indent=2) + "\n"
        if args.json_out:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(output, encoding="utf-8")
        print(output, end="")
    except (OSError, ValueError) as error:
        parser.exit(1, f"TPC-H ABBA benchmark failed: {error}\n")
    return 0 if report["results"] and all(r["status"] == "ok" for r in report["results"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())

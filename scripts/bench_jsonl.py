#!/usr/bin/env python3
"""Run deterministic newline-delimited JSON SQL workloads against RustSpark."""

import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import time

from bench_rustspark import WORKLOADS, benchmark_row, expected_results, output_signature
from bench_stats import summarize_samples


def encode_jsonl_row(index):
    city, amount = benchmark_row(index)
    return json.dumps({"id": index, "city": city, "amount": amount}) + "\n"


def write_jsonl(path, rows):
    if rows < 1:
        raise ValueError("rows must be positive")
    with Path(path).open("w", encoding="utf-8") as stream:
        stream.writelines(encode_jsonl_row(index) for index in range(rows))


def command(binary, file, sql):
    return [str(binary), "--json", str(file),
            "--json-schema", "id:int64,city:utf8,amount:int64",
            "--sql", sql, "--stdout-format", "json"]


def measure(binary, file, sql, digest, repeat, warmup, timeout):
    samples = []
    for attempt in range(repeat + warmup):
        started = time.perf_counter()
        run = subprocess.run(command(binary, file, sql),
                             capture_output=True, text=True, timeout=timeout, check=True)
        elapsed = (time.perf_counter() - started) * 1000
        actual, _ = output_signature(run.stdout)
        if actual != digest:
            raise ValueError("RustSpark JSONL query result differs from the expected output")
        if attempt >= warmup:
            samples.append(elapsed)
    return {**summarize_samples(samples), "result_sha256": digest}


def benchmark(binary, rows=100000, repeat=3, warmup=1, timeout=120):
    if rows < 1 or repeat < 1 or warmup < 0 or timeout < 1:
        raise ValueError("Invalid JSONL benchmark settings")
    with tempfile.TemporaryDirectory(prefix="rustspark-jsonl-") as folder:
        file = Path(folder) / "events.ndjson"
        write_jsonl(file, rows)
        expected = expected_results(rows)
        results = []
        for name, sql in WORKLOADS.items():
            digest = output_signature("\n".join(json.dumps(row) for row in expected[name]))[0]
            results.append({
                "name": name, "sql": sql,
                **measure(binary, file, sql, digest, repeat, warmup, timeout),
            })
        return {"metric": "cold CLI + JSONL input + SQL + JSON output",
                "rows": rows, "json_bytes": file.stat().st_size,
                "repeat": repeat, "warmup": warmup, "workloads": results}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--json-out", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        result = benchmark(args.binary, args.rows, args.repeat, args.warmup, args.timeout)
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        parser.exit(1, f"JSONL benchmark failed: {error}\n")
    print(f"Verified {len(result['workloads'])} JSONL SQL workloads")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

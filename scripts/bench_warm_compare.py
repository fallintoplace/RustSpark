#!/usr/bin/env python3
"""Compare warmed local SQL sessions, not cold CLI/JVM startup times."""
import argparse
import importlib.util
import json
import math
import pathlib
import platform
import re
import statistics
import subprocess
import sys
import tempfile

from bench_rustspark import (WORKLOADS, expected_results, output_signature, percentile, write_csv,
                            _unique_json_keys, _reject_nonfinite_json)

TIMER = re.compile(r"^Time:\s+(\d+(?:\.\d+)?)\s+ms$", re.MULTILINE)
SPARK_WORKER = """
import json, sys, time
from pyspark.sql import SparkSession, types

csv, master = sys.argv[1:]
requests = json.loads(sys.stdin.read())
spark = (SparkSession.builder.master(master)
    .appName("RustSpark warm SQL benchmark")
    .config("spark.sql.session.timeZone", "UTC").getOrCreate())
try:
    schema = types.StructType([
        types.StructField("id", types.LongType(), True),
        types.StructField("city", types.StringType(), True),
        types.StructField("amount", types.LongType(), True),
    ])
    spark.read.option("header", "true").schema(schema).csv(
        csv).createOrReplaceTempView("data")
    for request in requests:
        started = time.perf_counter()
        rows = [row.asDict(recursive=True)
                for row in spark.sql(request["sql"]).collect()]
        encoded = json.dumps(rows, default=str)
        elapsed = (time.perf_counter() - started) * 1000
        print(json.dumps({
            "name": request["name"], "elapsed_ms": elapsed,
            "rows": json.loads(encoded),
        }), flush=True)
finally:
    spark.stop()
"""

DUCKDB_WORKER = """
import json, sys, time
import duckdb

csv = sys.argv[1]
requests = json.loads(sys.stdin.read())
con = duckdb.connect(":memory:")
escaped = csv.replace("'", "''")
con.execute("CREATE VIEW data AS SELECT * FROM read_csv('" + escaped
            + "', header=true, columns={'id':'BIGINT',"
            + "'city':'VARCHAR','amount':'BIGINT'})")
for request in requests:
    started = time.perf_counter()
    cursor = con.execute(request["sql"])
    columns = [field[0] for field in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    encoded = json.dumps(rows, default=str)
    elapsed_ms = (time.perf_counter() - started) * 1000
    print(json.dumps({
        "name": request["name"], "elapsed_ms": elapsed_ms,
        "rows": json.loads(encoded),
    }), flush=True)
"""


def parse_result_line(line):
    return json.loads(
        line, object_pairs_hook=_unique_json_keys, parse_constant=_reject_nonfinite_json,
    )


def requests_for(warmup, repeat):
    return [{"name": name, "sql": sql}
            for name, sql in WORKLOADS.items()
            for _ in range(warmup + repeat)]


def parse_rustspark(stdout, stderr, requests, row_count):
    if "SQL error:" in stderr or "Shell error:" in stderr:
        raise RuntimeError("RustSpark SQL failed:\n" + stderr)
    times = [float(value) for value in TIMER.findall(stderr)]
    if len(times) != len(requests):
        raise ValueError(f"Got {len(times)} timers for {len(requests)} SQL statements")
    data = [parse_result_line(line) for line in stdout.splitlines() if line.strip()]
    expected = expected_results(row_count)
    offset = 0
    samples = []
    for request, ms in zip(requests, times):
        size = len(expected[request["name"]])
        batch = data[offset:offset + size]
        if len(batch) != size:
            raise ValueError(f"Missing result rows for {request['name']}")
        samples.append({"name": request["name"], "elapsed_ms": ms, "rows": batch})
        offset += size
    if offset != len(data):
        raise ValueError("Unexpected extra RustSpark result rows")
    return samples


def run_rustspark(binary, csv, requests, row_count, timeout, partitions):
    script = ".timer on\n" + "\n".join(
        item["sql"].rstrip(";") + ";" for item in requests
    ) + "\n.quit\n"
    command = [
        binary, "--csv", str(csv), "--csv-schema",
        "id:int64,city:utf8,amount:int64", "--target-partitions",
        str(partitions), "--interactive", "--stdout-format", "json",
    ]
    output = subprocess.run(
        command, input=script, capture_output=True, text=True,
        check=False, timeout=timeout,
    )
    if output.returncode:
        raise RuntimeError(f"RustSpark exit {output.returncode}: {output.stderr}")
    return parse_rustspark(output.stdout, output.stderr, requests, row_count)


def run_spark(csv, requests, timeout, master):
    if importlib.util.find_spec("pyspark") is None:
        raise RuntimeError("Install PySpark 4.x: pip install 'pyspark>=4,<5'")
    output = subprocess.run(
        [sys.executable, "-c", SPARK_WORKER, str(csv), master],
        input=json.dumps(requests), capture_output=True, text=True,
        check=False, timeout=timeout,
    )
    if output.returncode:
        raise RuntimeError(f"PySpark exit {output.returncode}: {output.stderr}")
    return [parse_result_line(line) for line in output.stdout.splitlines() if line.strip()]


def run_duckdb(csv, requests, timeout):
    if importlib.util.find_spec("duckdb") is None:
        raise RuntimeError("Install DuckDB: pip install 'duckdb>=1,<3'")
    output = subprocess.run(
        [sys.executable, "-c", DUCKDB_WORKER, str(csv)],
        input=json.dumps(requests), capture_output=True, text=True,
        check=False, timeout=timeout,
    )
    if output.returncode:
        raise RuntimeError(f"DuckDB exit {output.returncode}: {output.stderr}")
    return [parse_result_line(line) for line in output.stdout.splitlines() if line.strip()]


def summarize(samples, requests, row_count, warmup, repeat):
    if len(samples) != len(requests):
        raise ValueError(f"Expected {len(requests)} results, got {len(samples)}")
    fingerprints = {
        name: output_signature("\n".join(json.dumps(row) for row in data))[0]
        for name, data in expected_results(row_count).items()
    }
    by_workload = {name: [] for name in WORKLOADS}
    for sample, request in zip(samples, requests):
        name = request["name"]
        if sample["name"] != name:
            raise ValueError(f"Unexpected query order: {sample['name']} instead of {name}")
        ms = sample["elapsed_ms"]
        if (isinstance(ms, bool) or not isinstance(ms, (float, int))
                or not math.isfinite(ms) or ms < 0):
            raise ValueError(f"Invalid duration for {name}: {ms}")
        digest = output_signature(
            "\n".join(json.dumps(row) for row in sample["rows"])
        )[0]
        if digest != fingerprints[name]:
            raise RuntimeError(f"Incorrect result for {name}: {sample['rows']!r}")
        by_workload[name].append(float(ms))
    report = {}
    for name, durations in by_workload.items():
        if len(durations) != warmup + repeat:
            raise ValueError(f"Missing samples for {name}")
        measured = durations[warmup:]
        report[name] = {
            "median_ms": round(statistics.median(measured), 3),
            "p95_ms": round(percentile(measured, 0.95), 3),
            "samples_ms": [round(value, 3) for value in measured],
            "result_sha256": fingerprints[name],
        }
    return report


def competitor_median_ratios(reports, engine):
    if "rustspark" not in reports or engine not in reports:
        return {}
    return {
        name: (
            round(reports[engine][name]["median_ms"]
                  / reports["rustspark"][name]["median_ms"], 3)
            if reports["rustspark"][name]["median_ms"] > 0 else None
        )
        for name in WORKLOADS
    }


def benchmark(binary, engines, rows, warmup, repeat, timeout, partitions, master):
    if min(rows, repeat, timeout, partitions) < 1 or warmup < 0:
        raise ValueError("rows, repeat, timeout, partitions must be positive; warmup >= 0")
    if (not engines or len(engines) != len(set(engines))
            or any(engine not in ("rustspark", "spark", "duckdb") for engine in engines)):
        raise ValueError("Specify unique engines: rustspark, spark and/or duckdb")
    with tempfile.TemporaryDirectory(prefix="rustspark-warm-") as directory:
        csv = pathlib.Path(directory) / "input.csv"
        write_csv(csv, rows)
        requests = requests_for(warmup, repeat)
        reports = {}
        for engine in engines:
            if engine == "rustspark":
                samples = run_rustspark(binary, csv, requests, rows, timeout, partitions)
            elif engine == "spark":
                samples = run_spark(csv, requests, timeout, master)
            else:
                samples = run_duckdb(csv, requests, timeout)
            reports[engine] = summarize(samples, requests, rows, warmup, repeat)
        ratios = competitor_median_ratios(reports, "spark")
        duckdb_ratios = competitor_median_ratios(reports, "duckdb")
        return {
            "metric": "Warm local SQL planning, execution and JSON serialization",
            "warning": "Excludes startup, input registration and distributed execution",
            "rows": rows, "csv_bytes": csv.stat().st_size,
            "warmup": warmup, "repeat": repeat, "partitions": partitions,
            "spark_master": master, "platform": platform.platform(),
            "workloads": WORKLOADS, "results": reports,
            "spark_median_over_rustspark_median": ratios,
            "duckdb_median_over_rustspark_median": duckdb_ratios,
        }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--engines", nargs="+", choices=("rustspark", "spark", "duckdb"),
                        default=["rustspark", "spark"])
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--target-partitions", type=int, default=2)
    parser.add_argument("--spark-master", default="local[2]")
    parser.add_argument("--json-out", type=pathlib.Path)
    args = parser.parse_args(argv)
    report = benchmark(
        args.binary, args.engines, args.rows, args.warmup, args.repeat,
        args.timeout, args.target_partitions, args.spark_master,
    )
    output = json.dumps(report, indent=2) + "\n"
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(output, encoding="utf-8")
    print(output, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

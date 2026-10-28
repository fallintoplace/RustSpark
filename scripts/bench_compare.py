#!/usr/bin/env python3
"""Cold end-to-end RustSpark / DuckDB / PySpark SQL comparison.

Every timed run starts a new process, registers typed CSV, executes SQL,
and writes NDJSON. The metrics do not measure warm engine throughput.
"""
import argparse
import importlib.util
import json
import math
import pathlib
import platform
import subprocess
import sys
import tempfile
import time

from bench_rustspark import WORKLOADS, expected_results, output_signature, write_csv
from bench_stats import summarize_samples


DUCKDB_WORKER = """
import duckdb, json, sys
path, sql = sys.argv[1:]
con = duckdb.connect(':memory:')
escaped = path.replace("'", "''")
con.execute("CREATE VIEW data AS SELECT * FROM read_csv('" + escaped
            + "', header=true, columns={'id':'BIGINT','city':'VARCHAR','amount':'BIGINT'})")
result = con.execute(sql)
columns = [field[0] for field in result.description]
for row in result.fetchall():
    print(json.dumps(dict(zip(columns, row)), default=str))
"""

SPARK_WORKER = """
import json, sys
from pyspark.sql import SparkSession, types
path, sql = sys.argv[1:]
spark = (SparkSession.builder.master('local[2]')
    .appName('RustSpark benchmark')
    .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
try:
    schema = types.StructType([
        types.StructField('id', types.LongType(), True),
        types.StructField('city', types.StringType(), True),
        types.StructField('amount', types.LongType(), True),
    ])
    spark.read.option('header', 'true').schema(schema).csv(path).createOrReplaceTempView('data')
    for row in spark.sql(sql).collect():
        print(json.dumps(row.asDict(recursive=True), default=str))
finally:
    spark.stop()
"""


def command_for(engine, binary, csv, sql):
    if engine == "rustspark":
        return [binary, "--csv", str(csv), "--csv-schema",
                "id:int64,city:utf8,amount:int64", "--sql", sql,
                "--stdout-format", "json"]
    if engine == "duckdb":
        return [sys.executable, "-c", DUCKDB_WORKER, str(csv), sql]
    if engine == "spark":
        return [sys.executable, "-c", SPARK_WORKER, str(csv), sql]
    raise ValueError(f"Unknown benchmark engine: {engine}")


def check_dependencies(engines):
    for engine, package in [("duckdb", "duckdb"), ("spark", "pyspark")]:
        if engine in engines and importlib.util.find_spec(package) is None:
            raise RuntimeError(f"{engine} requires '{package}'")


def execute(command, timeout):
    started = time.perf_counter()
    output = subprocess.run(command, capture_output=True, text=True,
                            timeout=timeout, check=True)
    elapsed = (time.perf_counter() - started) * 1000
    digest, rows = output_signature(output.stdout)
    return elapsed, digest, rows


def measure_engine_workload(engine, workload, command, expected_digest, repeat, warmup, timeout):
    samples = []
    for iteration in range(warmup + repeat):
        elapsed, digest, actual = execute(command, timeout)
        if (isinstance(elapsed, bool) or not isinstance(elapsed, (float, int))
                or not math.isfinite(elapsed) or elapsed < 0):
            raise ValueError(f"Invalid benchmark duration for {engine}/{workload}: {elapsed}")
        if digest != expected_digest:
            raise RuntimeError(f"Incorrect result for {engine}/{workload}: {actual!r}")
        if iteration >= warmup:
            samples.append(round(elapsed, 3))
    return {**summarize_samples(samples), "result_sha256": expected_digest}


def _median_ratio(competitor, rustspark):
    if (not all(type(value) in (int, float) and math.isfinite(value) and value >= 0
                for value in (competitor, rustspark))):
        raise ValueError("Median latencies must be finite and nonnegative")
    if rustspark == 0:
        return None
    ratio = competitor / rustspark
    if not math.isfinite(ratio):
        raise ValueError("Cross-engine latency ratio overflow")
    return round(ratio, 3)


def competitor_median_ratios(results, engines):
    if "rustspark" not in results:
        return {}
    return {
        engine: {
            workload: (
                _median_ratio(measured["median_ms"], results["rustspark"][workload]["median_ms"])
            )
            for workload, measured in results[engine].items()
        }
        for engine in engines if engine != "rustspark"
    }


def _benchmark_engine(binary, engine, csv, expected, repeat, warmup, timeout):
    results = {}
    for workload, query in WORKLOADS.items():
        results[workload] = measure_engine_workload(
            engine, workload, command_for(engine, binary, csv, query),
            expected[workload], repeat, warmup, timeout,
        )
    return results


def benchmark(binary, engines, rows, repeat, warmup, timeout):
    if rows < 1 or repeat < 1 or warmup < 0 or timeout < 1:
        raise ValueError("rows, repeat and timeout must be positive; warmup >= 0")
    if (not engines or len(engines) != len(set(engines))
            or any(engine not in ("rustspark", "duckdb", "spark") for engine in engines)):
        raise ValueError("Select unique engines from rustspark, duckdb and spark")
    check_dependencies(engines)
    with tempfile.TemporaryDirectory(prefix="rustspark-compare-") as directory:
        csv = pathlib.Path(directory) / "input.csv"
        write_csv(csv, rows)
        expected = {
            name: output_signature("\n".join(json.dumps(row) for row in results))[0]
            for name, results in expected_results(rows).items()
        }
        results = {
            engine: _benchmark_engine(binary, engine, csv, expected, repeat, warmup, timeout)
            for engine in engines
        }
        ratios = competitor_median_ratios(results, engines)
        return {
            "metric": "cold process startup + typed CSV registration + SQL + JSON output",
            "warning": "Not warm engine or distributed Spark throughput",
            "rows": rows, "csv_bytes": csv.stat().st_size,
            "warmup": warmup, "repeat": repeat,
            "platform": platform.platform(), "python": platform.python_version(),
            "engines": engines, "workloads": WORKLOADS, "results": results,
            "competitor_median_over_rustspark_median": ratios,
        }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="./target/release/rustspark")
    parser.add_argument("--engines", nargs="+", choices=["rustspark", "duckdb", "spark"],
                        default=["rustspark", "duckdb"])
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--json-out", type=pathlib.Path)
    args = parser.parse_args(argv)
    result = benchmark(args.binary, args.engines, args.rows, args.repeat,
                       args.warmup, args.timeout)
    rendered = json.dumps(result, indent=2) + "\n"
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())

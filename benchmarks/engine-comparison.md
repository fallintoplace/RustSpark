# Comparing RustSpark, DuckDB and local Spark SQL

This optional harness extends the existing RustSpark SQL benchmark with real
DuckDB and PySpark processes.

Run: install DuckDB and PySpark 4, build RustSpark with cargo build --release
--locked, then execute:

    python3 scripts/bench_compare.py --binary ./target/release/rustspark --engines rustspark duckdb spark --rows 100000 --repeat 5 --json-out benchmarks/engine-comparison.json

Each engine receives the same deterministic CSV and explicit column types,
runs identical SQL and must return identical normalized JSON results.
Any mismatch fails the benchmark before results are recorded.
Samples, medians, p95, query texts and environment details are saved in JSON.

Every sample starts a new process. Results include initialization, input
registration, query and serialization, not warmed engine-only throughput.
Spark runs local[2], not a remote cluster. The comparison GitHub workflow
runs on manual request; normal PR CI only runs stdlib harness unit tests.

## Warm SQL session benchmark

The existing cold comparison includes process startup and Spark JVM startup.
Use a separate warm-session comparison to measure repeated local SQL:

```sh
cargo build --release --locked
python3 -m pip install 'pyspark>=4,<5'
python3 scripts/bench_warm_compare.py --binary ./target/release/rustspark \
  --engines rustspark spark --rows 100000 --warmup 1 --repeat 5 \
  --json-out benchmarks/warm-comparison.json
```

This keeps one RustSpark shell and one PySpark session alive per engine.
RustSpark uses `.timer on`; PySpark uses an in-process timer.
Planning, query execution and JSON encoding are included, but process
startup and input registration are not. All samples are checked against
deterministic expected results, including multi-row GROUP BY output.
The workload uses typed CSV and local Spark `local[2]`; it is **not**
a distributed, Parquet lake, or fault-tolerance benchmark.
The manual `Compare warm local SQL sessions` workflow uploads raw timings.

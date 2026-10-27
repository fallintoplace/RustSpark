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

## Scheduled comparisons

GitHub Actions runs the cold comparison every Monday and the warm comparison
every Tuesday. The runs use the manual workflow defaults and upload checked
JSON and Markdown reports for 90 days. Manual runs can override row count
and repetition count.

GitHub-hosted runners vary, so these reports are observations, not a
statistically reliable performance regression gate. Compare repeated runs on
controlled hardware before claiming a speedup.

## Reading benchmark results

The optional cold and warm comparison workflows upload measured JSON **and**
a readable Markdown report. The summary includes per-workload median and p95
latencies, verified cross-engine result fingerprints, and speed ratios.
For a ratio of competitor median / RustSpark median, **above 1x** means
RustSpark ran faster in that workload on that machine. The raw JSON fields
`competitor_median_over_rustspark_median` (cold) and
`spark_median_over_rustspark_median` (warm) use the same direction as the
Markdown report. A zero RustSpark median produces `null`, not a misleading
speed ratio.

Generate a report locally after either harness completes:

```sh
python3 scripts/bench_report.py benchmarks/engine-comparison.json \
  --output benchmarks/reports/cold.md
python3 scripts/bench_report.py benchmarks/warm-comparison.json \
  --output benchmarks/reports/warm.md
```

The renderer rejects missing workloads, unequal result fingerprints, invalid
timings, or summaries that disagree with their timing samples. Neither these
reports nor the harness establish distributed performance or statistically
significant speedups. Keep cold-start and warm-session results separate.

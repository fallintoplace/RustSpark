# Reproducible local SQL benchmarks

Use the central quality CLI to run the Python helper tests:

```sh
python3 scripts/quality.py test --suite all
```

It also launches the existing benchmark and reporting tools:

```sh
python3 scripts/quality.py bench local --rows 100000 --warmup 1 --repeat 5 --json-out benchmarks/local-results.json
python3 scripts/quality.py bench cold --engines rustspark duckdb --rows 100000
python3 scripts/quality.py bench report benchmarks/engine-comparison.json
```

The modes are `local`, `cold`, `warm`, `tpch`, `tpch-prepare`,
`clickbench`, `clickbench-fetch`, `report`, and `compare-reports`.
To get a tool's own help, use `bench <mode> -- --help`. Direct script
entry points remain supported.

Use this standalone harness to measure RustSpark as a user experiences it.
The harness generates deterministic CSV input, warms up each query and runs
repeated CLI queries. Query results must remain unchanged across repetitions.

```sh
cargo build --release --locked
python3 scripts/bench_rustspark.py --binary ./target/release/rustspark \
  --rows 100000 --warmup 1 --repeat 5 --json-out benchmarks/local-results.json
```

The four workloads cover sequential scan/count, filtered sum, grouped
aggregation, and an aggregate join. Queries use a fixed CSV schema to avoid
nondeterministic type inference. The dataset is generated outside timed runs.

JSON output records wall-clock latency samples, median, nearest-rank p95,
input size, SQL text, SHA-256 of normalized output rows, and environment info.
End-to-end latency **includes** RustSpark process startup, CSV registration,
planning, and execution. It is **not** an isolated query-engine benchmark.
For fair comparisons, use the same hardware, identical data, equivalent SQL
semantics, and similar CSV parsing configurations for every engine.

The harness is deliberately not run as a performance gate in PR CI, where
shared runners introduce too much noise. Its stdlib unit tests run in CI. Test modules are registered in one checked
manifest: new `scripts/test_*.py` files must belong to a suite or CI fails.
Timed comparisons remain opt-in and are not performance gates.

A later distributed Ballista or Apache Spark comparison needs separate jobs
and workloads that account for scheduler startup and network costs. Do not
treat the runtime numbers of this harness as Spark-versus-RustSpark results.

To view the verified RustSpark vs DuckDB/Spark benchmark results as Markdown,
see [Reading benchmark results](engine-comparison.md#reading-benchmark-results).

## 12-query end-to-end latency suite

Run the 12-query RustSpark-only suite to see per-query execution times:

```sh
cargo build --release --locked
python3 scripts/bench_query_latency.py --binary ./target/release/rustspark \
  --rows 100000 --warmup 1 --repeat 3 \
  --json-out benchmarks/results/query-latency.json \
  --markdown-out benchmarks/results/query-latency.md
```

The suite covers row counts, selective filters, groups, DISTINCT, HAVING,
ORDER BY/LIMIT, window ranks, UNION ALL, self joins and aggregate joins. All
queries use a deterministic typed CSV dataset generated outside timing. An
independent Python result oracle verifies each warmup and measured sample. A
failed query is reported without preventing later queries from running, and
the command exits nonzero if any query fails.

Use `--only top_k self_join` to measure just selected queries. The JSON saves
individual samples, median, nearest-rank p95, SQL, result hashes, input
checksum, and the runner environment. The Markdown file shows timing per
query in milliseconds. Query names are in `scripts/bench_query_latency.py`.

**Metric:** Each sample is a new RustSpark CLI process. Timings include CLI
startup, typed CSV registration, SQL planning/execution and JSON output. They
do not isolate warm engine execution time. The dedicated
`RustSpark query latency` GitHub Actions workflow runs when these benchmark
files change in a pull request, and can also be triggered manually after the
workflow is available on the default branch. The workflow uploads raw JSON
and the readable Markdown report for 90 days. Shared runner timings are
informational only and must not be treated as a strict performance gate.

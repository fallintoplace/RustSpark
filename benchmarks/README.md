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

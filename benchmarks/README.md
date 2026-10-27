# Reproducible local SQL benchmarks

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
shared runners introduce too much noise. Its stdlib unit tests run in CI.

A later distributed Ballista or Apache Spark comparison needs separate jobs
and workloads that account for scheduler startup and network costs. Do not
treat the runtime numbers of this harness as Spark-versus-RustSpark results.

To view the verified RustSpark vs DuckDB/Spark benchmark results as Markdown,
see [Reading benchmark results](engine-comparison.md#reading-benchmark-results).

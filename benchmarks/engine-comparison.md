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

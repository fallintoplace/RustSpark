# RustSpark native release acceptance

RustSpark 0.1.x is an experimental native SQL engine, **not** a production
persistent database or an Apache Spark-compatible distributed runtime.
Do not present a passing test as proof of ACID durability or Spark parity.

## Native preview release gates

- Validate the exact candidate commit on Linux, Windows, and macOS with the
  root CLI and standalone `native-engine/Cargo.toml` test suites.
- Run Clippy for both packages, with warnings denied.
- Verify that neither active package resolves DataFusion or Ballista.
- Exercise the packaged **native** CLI across processes with CSV, Parquet,
  joins, aggregation, and unsupported-SQL rejection.
- Verify streaming CLI memory limits, cancellation, external sort, and
  scratch-file cleanup with representative inputs.
- Record known SQL correctness gaps, results against a reference engine,
  and the exact revision and workload used for any performance claims.
- Complete the dependency security audit for both dependency graphs.
- Review artifact provenance and checksums before publishing any binary.

## Run local checks

```sh
cargo test --all-targets
cargo test --manifest-path native-engine/Cargo.toml --all-targets
cargo clippy --all-targets -- -D warnings
cargo clippy --manifest-path native-engine/Cargo.toml --all-targets -- -D warnings
python3 scripts/check_native_dependencies.py
```

## Production database gates still missing

The default native CLI does not provide a durable SQL data store. A production
database milestone additionally needs:

1. A versioned persistent catalog with schema validation and crash recovery.
2. Atomic data and metadata commits, restart tests, and documented fsync rules.
3. Multi-process access control, concurrency semantics, backups, and restore.
4. Bounded resident memory and scratch space for supported query operators.
5. Safe service authentication and authorization if network access is added.
6. Query correctness comparisons and repeatable realistic performance tests.

Until those requirements are implemented and qualified, describe RustSpark as
an experimental analytical query tool rather than a production database.

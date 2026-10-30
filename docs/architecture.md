# RustSpark native architecture and production boundaries

RustSpark 0.1.x is an experimental local analytical SQL engine. The default
binary and public Rust library use the independent native engine in
`native-engine/`. The retired DataFusion implementation is not a fallback.
It is not an ACID database or a drop-in Apache Spark replacement.

## Current query path

1. `native-engine/src/main.rs` parses the default CLI arguments and registers
   CSV or Parquet input into the native in-memory `Catalog`.
2. `native-engine/src/lexer.rs` and `sql.rs` tokenize and parse supported
   SQL into native logical plans.
3. `plan.rs`, `optimizer.rs`, and `executor.rs` plan and run local
   operators over native columnar `Batch` values.
4. `native-engine/src/scale/` implements experimental bounded streaming,
   CSV and Parquet file-backed sources, external sort, spill, grouped aggregates,
   and joins.
5. The alternate `rustspark-stream` binary uses `FileCatalog` and
   `stream_execute` for supported streaming queries.

Unsupported SQL must fail explicitly. The in-memory CLI can still materialize
whole data sources. The streaming engine has separate operator limitations.

## Storage and durability

The current `Catalog` stores data in memory; `FileCatalog` registers paths to
existing files. Neither is a transactional persistent SQL catalog. The native
CLI has **no** guaranteed WAL, crash recovery, fsync protocol, snapshot
publication, concurrent-writer control, or integrated backup and restore.

Adding a real persistent database requires a documented on-disk format,
exclusive-writer or concurrency protocol, atomic manifest publication, crash
injection, integrity checks, recovery, and independent backup qualification.
A process-crash test alone does not imply power-loss durability.

## Security and resource ownership

- Treat SQL and input files as untrusted when running with elevated privileges.
- Keep per-query scratch directories private and restrict temporary file access.
- Bound memory, row counts, output bytes, spill space, and plan complexity.
- Propagate cancellation and release file handles before deleting spills.
- Never claim isolated multi-tenant operation without authentication,
  authorization, resource quotas, and hostile-input tests.

## Performance qualification

Benchmarks must run against the **native** engine and record a fixed commit,
workload queries, input row counts and checksums, hardware, budget settings,
warmup policy, repetitions, medians, tail latency, and independently verified
SQL results. Historical DataFusion numbers are not evidence of native speed.

## Development checks

```sh
cargo test --all-targets
cargo test --manifest-path native-engine/Cargo.toml --all-targets
cargo clippy --all-targets -- -D warnings
cargo clippy --manifest-path native-engine/Cargo.toml --all-targets -- -D warnings
```

Require the completed checks for the exact release commit. Path-filtered or
queued workflows are not a substitute for a passing release gate.

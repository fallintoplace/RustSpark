# RustSpark native engine

This is a separate Rust crate for the planned **DataFusion-free execution path**.
It must never import `datafusion` or depend on the root `rustspark` crate.

The first migration phase builds and tests our own schema, typed columnar batches,
expression evaluation, logical plans, physical execution and a small SQL frontend.
The existing DataFusion-powered CLI is unchanged while this backend matures.

Run the native checks from the repository root:

```sh
cargo test --manifest-path native-engine/Cargo.toml --all-targets
```

## Release gates

1. Differential correctness against the existing engine for supported queries,
   including NULLs, overflow, joins and ordering.
2. Bounded memory use, cancellation, query deadlines and disk spill support.
3. Arrow/Parquet integration and compatible CSV/JSON ingestion.
4. TPC-H and ClickBench result checks with comparable timing protocols.
5. Port the public API and CLI, then remove DataFusion and Ballista dependencies.

Do not claim Spark parity or better-than-Spark performance until these gates pass.
The native crate is experimental and is not the default RustSpark engine.

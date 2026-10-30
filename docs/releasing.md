# Releasing native RustSpark binaries

RustSpark 0.1.x uses a native SQL engine, not the retired DataFusion CLI.
The root Cargo package depends on the standalone `native-engine/` crate.
The package is marked `publish = false`.

There is **no active automated binary release workflow** in this repository.
Do not assume that tagging a commit will publish signed archives or run a
storage recovery qualification. Release artifacts must be built and checked
separately until a native release workflow is implemented.

## Before publishing

1. Choose a commit on `main` after all intended PRs are merged.
2. Confirm completed, successful Linux, macOS, and Windows native Rust CI
   runs for that exact commit. Queued, cancelled, or skipped jobs do not pass.
3. Check the root and standalone native dependency graphs for forbidden
   DataFusion or Ballista dependencies and known RustSec advisories.
4. Build the current native binary and run CSV and Parquet smoke queries
   against its exact executable, including an invalid-query failure case.
5. Run the standalone native engine's tests and Clippy.
6. Record the commit hash, Rust toolchain, target triple, and source version.
7. Package the binary with the Apache-2.0 LICENSE, README, platform name,
   and independently checked SHA-256 checksums.

## Local qualification

```sh
cargo test --all-targets
cargo test --manifest-path native-engine/Cargo.toml --all-targets
cargo clippy --all-targets -- -D warnings
cargo clippy --manifest-path native-engine/Cargo.toml --all-targets -- -D warnings
python3 scripts/check_native_dependencies.py
cargo build --release --bin rustspark
cargo run -- --csv tests/fixtures/sales.csv --table sales \
  --sql 'SELECT city, SUM(amount) AS total FROM sales GROUP BY city ORDER BY city'
```

Do not use `--locked` as a release claim until all required package lockfiles
are intentionally generated and committed.

## Artifact handling

Build from an immutable commit, document checksums, exercise the extracted
package rather than only the build tree, and retain the exact generated
artifacts for reproducibility. Do not overwrite existing release tags.

## Release notes

Describe the CLI as an **experimental native Rust SQL query engine**.
There is no production-grade persistent database, ACID transaction protocol,
automatic backup, distributed query service, or Apache Spark compatibility
guarantee. State measured correctness and performance limits explicitly.

See [native release acceptance](v1-acceptance.md) and
[architecture](architecture.md).

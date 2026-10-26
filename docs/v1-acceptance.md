# RustSpark V1 acceptance

RustSpark is an **experimental local SQL engine**, built on Apache DataFusion.
A V1 local release does not promise Spark API compatibility, a distributed
scheduler, ACID transactions, or crash-atomic `INSERT INTO` operations.

## Release gates

- All existing CI jobs pass, including formatting, Clippy, Rust 1.95 and Windows.
- The `v1_workflows` integration suite passes on Linux and Windows.
- Persistent tables can be created, appended to, queried after a process
  restart, renamed, and dropped without losing successful prior operations.
- Named CSV sources can be queried together with SQL aggregation, and typed
  parameters are handled correctly.
- Failures return a nonzero exit status. Successful queries emit the expected
  CSV output for shell pipelines.
- Existing storage concurrency and recovery PRs have been merged and verified
  before recommending persistent use.
- Benchmark results and known Spark SQL differences are documented, when the
  optional comparison and benchmark harnesses are available.
- The release archive is tested on a clean environment before publishing.

## Run the core checks

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
cargo test --locked --test v1_workflows
```

The tests deliberately launch the real CLI in separate processes rather than
calling the library directly. This catches command-line option changes, output
format regressions and on-disk catalog problems that unit tests can miss.

## Limitations to publish with V1

- File-backed `INSERT INTO` is append-only and not transactional.
- A failed insert or process crash can leave partially written data. Do not
  store irreplaceable data in the local catalog.
- Multiple simultaneous writers are not supported; the pending database lock
  is exclusive even for read-only persistent sessions.
- Distributed execution remains experimental and opt-in.
- RustSpark is not a drop-in replacement for Apache Spark.

A release must describe these limits prominently until they are resolved.

# Optional RustSpark + Ballista 55 remote execution

This is a separate Cargo project to avoid adding distributed dependencies to
RustSpark's default CLI or changing the root Cargo.lock.

It demonstrates an **actual Ballista remote SessionContext** wrapped by the
RustSpark session API. This is a feasibility spike, not a production distributed
mode or a claim of Apache Spark SQL compatibility.

## Run with a Ballista 55 scheduler and executor

Install / start a compatible scheduler and executor on the same machine:

```sh
cargo install --locked ballista-scheduler --version 55.0.0
cargo install --locked ballista-executor --version 55.0.0

# Terminal 1
ballista-scheduler

# Terminal 2
ballista-executor -c 2

# Terminal 3, from RustSpark repository root
cargo run --manifest-path integrations/ballista/Cargo.toml
```

The default query uses in-memory values, so no shared dataset is required.
For a Parquet query accessible to **every** remote executor:

```sh
BALLISTA_URL=df://localhost:50050 \
RUSTSPARK_PARQUET=/shared/events.parquet \
RUSTSPARK_SQL='SELECT COUNT(*) FROM data' \
cargo run --manifest-path integrations/ballista/Cargo.toml
```

### Limitations

- This is a separate, opt-in binary, not `rustspark --distributed`.
- Local-only input paths do not work on remote executors without shared storage.
- Distributed writes, server-side cancellation, authentication, timeout
  propagation, and scheduling policies are not exposed by this example.
  Client deadlines are described in [query-deadline.md](docs/query-deadline.md).
- A successful result does not establish that arbitrary SQL is compatible with
  Spark mainline, or that distributed performance is faster than local execution.
- Run the isolated integration manually using the instructions above. The
  project's default CI does not compile or download Ballista.
- The nested Cargo.lock is generated on first build, independently of the
  production crate. The exact Ballista/DataFusion versions are pinned.

Reference: https://datafusion.apache.org/ballista/user-guide/rust.html

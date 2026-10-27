# Optional RustSpark + Ballista 55 remote execution

This is a separate Cargo project to avoid adding distributed dependencies to
RustSpark's default CLI or changing the root Cargo.lock.

It demonstrates an **actual Ballista remote SessionContext** wrapped by the
RustSpark session API. This is a feasibility spike, not a production distributed
mode or a claim of Apache Spark SQL compatibility.

## In-process distributed mode for local development

Ballista can run an in-process scheduler with multiple executor slots. This
exercises distributed plan execution without first installing separate
scheduler and executor binaries:

```sh
RUSTSPARK_STANDALONE_PARALLELISM=4 \
RUSTSPARK_SQL='SELECT SUM(id) FROM (VALUES (1), (2), (3)) AS t(id)' \
cargo run --manifest-path integrations/ballista/Cargo.toml --features standalone --example standalone
```


The standalone example also supports `RUSTSPARK_INPUTS` with named CSV, JSON,
and Parquet sources, using the same registration logic as the remote client.
This allows testing distributed joins without a separate scheduler installation.

To validate a real join plus aggregation through Ballista, run this opt-in
smoke test after Rust is installed:

```sh
python3 scripts/ballista_smoke.py --mode standalone
```

For an existing remote scheduler and executor, first build the remote client,
then run the same query against the external services:

```sh
cargo build --manifest-path integrations/ballista/Cargo.toml
python3 scripts/ballista_smoke.py --mode remote \
  --client integrations/ballista/target/debug/rustspark-ballista-remote
```

The remote test requires all executors to see the same fixture directory
(the script uses a temporary local directory). It is suitable for shared-host
smoke checks, not as proof of multi-host storage or recovery correctness.

The default standalone parallelism is two. Values above 256 are rejected to
avoid starting too many in-process executors. `RUSTSPARK_PARQUET` can point to
a dataset visible to the process. Standalone mode does **not** validate
multi-host scheduling, worker failure handling, or shared-storage access.
Use the remote client above for tests across machines.

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

### Register several distributed tables

Use `RUSTSPARK_INPUTS` to register named Parquet, CSV, or newline-delimited
JSON inputs on the remote session. This enables joins and aggregations across
different sources without collecting them in the client:

```sh
BALLISTA_URL=df://scheduler:50050 \
RUSTSPARK_INPUTS='parquet:orders=/shared/orders;csv:customers=/shared/customers.csv' \
RUSTSPARK_SQL='SELECT COUNT(*) FROM orders JOIN customers USING (customer_id)' \
cargo run --manifest-path integrations/ballista/Cargo.toml
```

Entries are separated by semicolons and use `format:table=path`. Table names
must be simple SQL identifiers and are unique ignoring case. Paths must not
contain semicolons. The older `RUSTSPARK_PARQUET` still registers `data`;
`RUSTSPARK_INPUTS` cannot also use that name. All paths or object store
locations must be accessible from the worker nodes. This example does not
configure object store credentials.

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

# Run RustSpark in a rootless container

Build the local image:

```sh
docker build -t rustspark:local .
```

Run a query against a CSV file from a read-only mounted directory:

```sh
docker run --rm --network none \
  --mount "type=bind,source=$PWD/tests/fixtures,target=/data,readonly" \
  rustspark:local --csv /data/sales.csv \
  --sql 'SELECT city, SUM(amount) AS total FROM data GROUP BY city'
```

For Parquet input, pass `--parquet /data/example.parquet`; for
NDJSON, pass `--json /data/example.json`. Pass an explicit
`--memory-limit-mb` for memory-constrained workloads and mount a
separate writable volume for spill data if needed.

The image runs as UID 10001, without a root shell or Docker daemon.
Input mounts are read-only in the example. If you mount a writable
`--database-dir`, grant UID 10001 access and back up its contents.
Do not expose persistent storage over a network share that violates
the local single-writer assumptions. This is a packaging example,
not a multi-tenant database or a public SQL server.

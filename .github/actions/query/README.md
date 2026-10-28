# RustSpark query composite action

Run a local query in GitHub Actions after installing or building the
RustSpark executable. The action does not download an unverified binary.

```yaml
steps:
  - uses: actions/checkout@v7
  - run: cargo build --release --locked
  - id: rustspark
    uses: ./.github/actions/query
    with:
      binary: ./target/release/rustspark
      input: tests/fixtures/sales.csv
      sql: SELECT COUNT(*) AS n FROM data
      format: json-array
      max-rows: '10'
  - run: cat "$RESULT"
    env:
      RESULT: ${{ steps.rustspark.outputs.result-file }}
```

Each invocation writes to a unique `RUNNER_TEMP` file. Paths, SQL
text and typed values are passed through shell-safe argument arrays.
There is no PR-provided executable download or arbitrary file overwrite
option. Results exist only for the job lifetime.

This action runs RustSpark queries but does not make CI execution a
production SQL service. Use `sql-file` for reviewed queries and do not
execute untrusted SQL with sensitive filesystem access.

# Spark SQL compatibility baseline

RustSpark uses DataFusion and does not currently claim Spark SQL compatibility.
This opt-in harness runs each read-only, input-free query against the RustSpark
CLI and an actual local Spark SQL engine, then compares column names and
normalized result rows. Differences are surfaced rather than silently accepted.

## Run

Build RustSpark first:

```sh
cargo build --locked
python3 -m pip install 'pyspark>=4,<5'
python3 scripts/spark_compat.py --rustspark target/debug/rustspark
```

Run a single case with `--case group_sum`. Use `--spark-master local[4]`
to change the Spark local parallelism. For CI harness unit tests without Spark:

```sh
python3 -m unittest discover -s scripts -p 'test_spark_compat.py'
```

## Coverage and limits

- The initial corpus contains 30 deterministic, source-free SQL cases:
  casts, NULL behavior, expressions, joins, grouping, unions, windows and sorting.
- SQL result rows are compared as multisets unless `ordered: true` is declared,
  because SQL without ORDER BY has no guaranteed order.
- Output column names and JSON scalar value types are compared. The harness
  does **not yet** compare Spark and Arrow logical column types, nullability,
  error classes, timezones, data from tables, or distributed execution.
- A query that errors on either side is counted separately as ERROR, not PASS.
- The corpus is a differential discovery tool. Do not claim a compatibility
  percentage from it until both engines actually run on the same environment.
- Intentional divergences should be documented per case rather than removed
  silently from the corpus.

The Python harness is optional; default Rust CI does not install Spark.

## Table-backed typed differential checks

The additional optional harness `scripts/spark_typed_compat.py` compares
the same two CSV fixtures in RustSpark and actual local Spark, using explicit
int64 and UTF-8 schemas. It covers joins, aggregation, decimals, timestamps,
NULLs and window functions. Both result values and normalized logical type
names are compared; discrepancies and execution errors are reported.

```sh
cargo build --locked
python3 -m pip install 'pyspark>=4,<5'
python3 scripts/spark_typed_compat.py --rustspark ./target/debug/rustspark \
  --json-out reports/spark-typed.json
```

The manually triggered and weekly GitHub workflow runs real PySpark and
publishes the JSON report even when differences occur. Regular PR CI runs only
the Python unit tests to avoid a large Spark download on every change.
This is a differential measurement tool, not a compatibility guarantee.

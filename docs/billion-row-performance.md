# Billion-row query performance

## Goal and definitions

RustSpark is an experimental **local** DataFusion-based SQL engine, not a
distributed replacement for Spark. The goal is to measure and improve
specified queries on >=1,000,000,000 source rows. No subsecond or seconds-level
performance claim is valid without a recorded dataset, query, hardware, result
checksum, and wall-clock measurement.

A metadata-only \`COUNT(*)\` can use row counts without scanning values.
Therefore this benchmark uses a full-column \`SUM\`, selective \`SUM\`, and
grouped \`SUM\` on TPC-H \`lineitem\`. These queries must evaluate column
data. The Parquet reader may still prune row groups for selective predicates.

For scale: a single uncompressed 64-bit column across 1 billion rows is
8,000,000,000 bytes (about 7.45 GiB). Reading it in 2 seconds requires
at least 4 GB/s of effective raw-column throughput, ignoring startup,
decompression, CPU, aggregation, and output. Disk bytes can be smaller
when the data compresses. This is a useful sanity check, not an achieved
benchmark result.

## Generate a large, reproducible input

This step needs substantial disk space, memory, and time. Install DuckDB
and its TPC-H extension prerequisites. The prepare script uses DuckDB's
official TPC-H generator. Do **not** generate this during regular PR CI.

~~~sh
python3 -m pip install duckdb
python3 scripts/tpch_prepare.py --scale-factor 167 --output /data/tpch-sf167
cargo build --release --locked
~~~

The manifest lists per-table source row counts and SHA-256 hashes of the
generated Parquet files. Verify that \`lineitem\` actually contains at least
1 billion rows before comparing results. The runner rejects smaller or
incomplete fixtures by checking the manifest. This is a manifest check, not
a second independent count of the source rows.

## Run the same workload repeatedly

~~~sh
python3 scripts/quality.py bench billion-tpch \
  --fixture /data/tpch-sf167 \
  --binary ./target/release/rustspark \
  --target-partitions 16 --batch-size 16384 \
  --parquet-pushdown-filters --warmup 1 --repeat 5 \
  --timeout 900 --json-out /data/rustspark-billion.json
~~~

This is **cold, end-to-end local CLI latency**: process startup, eight
Parquet table registrations, SQL planning, execution, and JSON output.
It is not warm-query throughput and is not a distributed benchmark.
The benchmark requires stable output digests across runs, but it does
not independently check aggregate values against DuckDB. For cross-engine
correctness use the repository's separate TPC-H verification harness.

For a direct physical-operator profile, use a single \`lineitem\` registration:

~~~sh
./target/release/rustspark \
  --parquet-table lineitem=/data/tpch-sf167/data/lineitem.parquet \
  --target-partitions 16 --batch-size 16384 \
  --parquet-pushdown-filters --parquet-reorder-filters \
  --sql "SELECT SUM(l_extendedprice) FROM lineitem WHERE l_shipdate >= DATE '1998-01-01'" \
  --explain-analyze
~~~

Use the profile to identify whether file scan, decoding, filtering,
aggregation, or output dominates. Run the same SQL and input with each
optional setting on and off; compare multiple repetitions on the same
controlled hardware. DataFusion defaults intentionally stay unchanged.

## Tuning path

1. Tune file and column I/O first: partition counts, Parquet filter
   pushdown, Bloom reads when the files actually contain Bloom filters,
   schema view arrays, predicate cache, footer fetch hints, and row selections.
2. For joins, compare build-side partition thresholds and dynamic IN-list
   filtering only when input cardinalities and join keys justify them.
   Larger IN lists cost more memory per partition.
3. For grouped scans, compare partial aggregation probe thresholds. High
   key cardinality can make partial aggregation counterproductive.
4. For ordered queries, profile in-memory sorting and spill reservations;
   putting spill files on fast local SSD may help at large cardinalities.
5. Consider smaller Parquet row groups and stronger pruning during a
   separate input layout experiment, but **never** mix input layout changes
   into a same-bytes engine A/B comparison.
6. Evaluate warm cache behavior separately from cold process runs. Do not
   compare a warmed RustSpark run with a cold competing engine.

## Evidence required for a performance claim

Record the exact SQL, revision, input SHA-256 hashes, number of source rows,
hardware and disk, input format/compression, worker/partition counts,
memory limits, median and p95, repeat count, and output fingerprint.
Report source rows scanned per second and physical bytes scanned per second
only when the respective measured counts are available; **do not** divide
manifest rows by elapsed time and label it scanned rows if pruning applies.

A query completes in "seconds" only if that particular result and its full
conditions have been measured. The opt-in settings in this series are
experiment controls, not individually proven speedups.

## Qualify a complete TPC-H evidence bundle

For a separate **all-22-query** qualification (not the three-query billion
`lineitem` tuning run above), use the same generated `sf167` fixture:

~~~sh
python3 scripts/tpch_run.py \
  --binary ./target/release/rustspark \
  --queries-dir /data/tpch-sf167/queries \
  --parquet-dir /data/tpch-sf167/data \
  --require-all --warmup 1 --repeat 5 --timeout 900 \
  --json-out /data/tpch-full-results.json
python3 scripts/bench_tpch_verify.py \
  --binary ./target/release/rustspark \
  --queries-dir /data/tpch-sf167/queries \
  --parquet-dir /data/tpch-sf167/data \
  --timeout 900 --json-out /data/tpch-full-verified.json
python3 scripts/bench_tpch_evidence.py \
  /data/tpch-sf167/manifest.json \
  /data/tpch-full-results.json \
  /data/tpch-full-verified.json --check-files \
  --json-out /data/tpch-qualified.json
~~~

The last command checks row counts, all SQL and result hashes, independent
DuckDB matches, and the reported timing samples. With `--check-files`, it
also rehashes all eight Parquet files in bounded chunks. This may take
substantial additional disk I/O. Without that flag, only manifest metadata
is validated. Neither result proves a cross-engine speedup.

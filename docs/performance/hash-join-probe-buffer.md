# buffer hash-join probe input while building

Specify `--hash-join-probe-buffer-mb N` to override DataFusion's execution.hash_join_buffering_capacity setting. The value is expressed in MiB.

Overlapping probe-side I/O with hash-table construction can shorten join latency for I/O-bound queries. Disable or reduce the buffer if dynamic filtering becomes less effective.

The setting is unchanged unless supplied. Benchmark joins on a release binary using a representative Parquet dataset, validate the query's results, and inspect plans with `--explain-analyze`. This option alone cannot guarantee queries over billions of rows will run in seconds.

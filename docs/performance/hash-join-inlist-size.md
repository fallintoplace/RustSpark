# tune hash-join dynamic IN-list byte budget

Specify `--hash-join-inlist-max-kb N` to override DataFusion's optimizer.hash_join_inlist_pushdown_max_size setting. The value is expressed in KiB.

A larger small-build IN-list budget can push more equality keys into Parquet pruning, at the cost of additional memory per partition.

The setting is unchanged unless supplied. Benchmark joins on a release binary using a representative Parquet dataset, validate the query's results, and inspect plans with `--explain-analyze`. This option alone cannot guarantee queries over billions of rows will run in seconds.

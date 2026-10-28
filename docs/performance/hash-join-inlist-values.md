# tune dynamic hash-join IN-list cardinality

Specify `--hash-join-inlist-max-values N` to override DataFusion's optimizer.hash_join_inlist_pushdown_max_distinct_values setting. The value is expressed in values.

Increasing the allowed distinct keys can improve pruning of selective joins, but may inflate filter evaluation and memory costs.

The setting is unchanged unless supplied. Benchmark joins on a release binary using a representative Parquet dataset, validate the query's results, and inspect plans with `--explain-analyze`. This option alone cannot guarantee queries over billions of rows will run in seconds.

# ClickBench integration

Fetch the official 43-query SQL corpus from a pinned ClickBench commit:

```sh
python3 scripts/clickbench_fetch.py --output /tmp/clickbench/queries.sql
```

The downloader verifies the exact upstream Git blob hash before saving. It does
not commit third-party SQL into this Apache-2.0 repository.

Source: https://github.com/ClickHouse/ClickBench/blob/36250a94d5ae6ce03df29b876c99478608119116/datafusion/queries.sql

ClickBench is licensed under **CC BY-NC-SA 4.0**, separately from RustSpark.
Read its [license](https://github.com/ClickHouse/ClickBench/blob/main/LICENSE)
before redistributing or using its materials. The query file remains outside
RustSpark's source tree and is not an Apache-2.0 artifact.

The official Parquet input is `https://datasets.clickhouse.com/hits_compatible/hits.parquet`.
The full dataset is not downloaded automatically or stored in this repository.

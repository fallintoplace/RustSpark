# Opt-in window TopN optimization

Use `--window-topn` for selective queries of the form `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) <= K`.

```sh
rustspark --parquet /data/events --window-topn --sql 'SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY timestamp DESC) AS rn FROM data) WHERE rn <= 3'
```

DataFusion 55.1 can maintain a bounded heap for each partition instead of fully sorting. This is **opt-in** because high-cardinality partition keys can increase memory use or regress performance. Verify with `--explain-analyze` and a representative dataset. It does not guarantee second-scale scans of billions of rows.

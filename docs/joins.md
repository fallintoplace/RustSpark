# JOIN USING in RustSpark

RustSpark uses Apache DataFusion for SQL query planning and execution. SQL
`INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL OUTER JOIN`, and `CROSS JOIN`
already work through `RustSparkSession::sql`.

For tables with a shared key name, the Rust API provides a lazy `USING` join:

```rust
use datafusion::logical_expr::JoinType;

let joined = session
    .join_tables_using("orders", "customers", JoinType::Left, &["customer_id"])
    .await?;
```

- The key is returned once, rather than once for each input table.
- The API accepts more than one key, for example `&["customer_id", "region"]`.
- A `FULL OUTER JOIN` retains keys from right-only rows.
- Regular SQL NULL matching applies: two NULL keys do not match.
- Non-key columns with duplicate names are not renamed automatically. Use an
  explicit SQL projection and aliases when a unique output schema is required.
- Only inner and outer joins are accepted by this helper. For semi, anti,
  non-equality, and self joins, use SQL with explicit predicates/aliases.
- Table paths and column names are quoted before SQL planning, so names such
  as `order` that are reserved SQL words can be used.

`join_tables_using` does not execute a join immediately. It returns a
DataFusion `DataFrame`, allowing further filtering, projection, streaming
or collection by the caller.

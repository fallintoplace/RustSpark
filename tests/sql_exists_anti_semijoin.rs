use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn exists_and_not_exists_distinguish_late_supplier_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "supplier",
            "SELECT 1 AS suppkey, 'Alpha' AS name UNION ALL SELECT 2, 'Beta'",
        )
        .await?;
    session
        .register_sql_view(
            "orders",
            "SELECT 100 AS orderkey, 'F' AS status UNION ALL SELECT 200, 'F'",
        )
        .await?;
    session
        .register_sql_view(
            "lineitem",
            "SELECT 100 AS orderkey, 1 AS suppkey, 5 AS commitday, 9 AS receiptday
         UNION ALL SELECT 100, 2, 5, 5
         UNION ALL SELECT 200, 1, 5, 9
         UNION ALL SELECT 200, 2, 5, 8",
        )
        .await?;
    let count = session
        .sql_scalar(
            "SELECT COUNT(*) FROM lineitem l1
         JOIN orders o ON o.orderkey = l1.orderkey
         JOIN supplier s ON s.suppkey = l1.suppkey
         WHERE o.status = 'F' AND l1.receiptday > l1.commitday
         AND EXISTS (
             SELECT 1 FROM lineitem l2
             WHERE l2.orderkey = l1.orderkey AND l2.suppkey <> l1.suppkey
         )
         AND NOT EXISTS (
             SELECT 1 FROM lineitem l3
             WHERE l3.orderkey = l1.orderkey
               AND l3.suppkey <> l1.suppkey
               AND l3.receiptday > l3.commitday
         ) AND s.name = 'Alpha'",
        )
        .await?;
    assert_eq!(count, ScalarValue::Int64(Some(1)));
    Ok(())
}

use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn grouped_join_with_having_filter_and_limit_keeps_correct_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session.register_sql_view("customer", "SELECT 1 AS custkey, 'Alpha' AS name UNION ALL SELECT 2, 'Beta'").await?;
    session.register_sql_view("orders", "SELECT 100 AS orderkey, 1 AS custkey, 900 AS totalprice UNION ALL SELECT 200, 2, 800").await?;
    session.register_sql_view("lineitem", "SELECT 100 AS orderkey, 160 AS qty UNION ALL SELECT 100, 151 UNION ALL SELECT 200, 200 UNION ALL SELECT 200, 50").await?;
    let count = session.sql_scalar(
        "SELECT COUNT(*) FROM (
            SELECT c.name, o.orderkey, SUM(l.qty) AS sum_qty
            FROM customer c JOIN orders o ON c.custkey = o.custkey
            JOIN lineitem l ON o.orderkey = l.orderkey
            WHERE o.orderkey IN (SELECT orderkey FROM lineitem GROUP BY orderkey HAVING SUM(qty) > 300)
            GROUP BY c.name, o.orderkey, o.totalprice
            ORDER BY o.totalprice DESC LIMIT 100
        ) result WHERE sum_qty = 311 AND name = 'Alpha'",
    ).await?;
    assert_eq!(count, ScalarValue::Int64(Some(1)));
    Ok(())
}

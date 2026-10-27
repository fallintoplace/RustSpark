use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn reused_aggregate_cte_keeps_correlated_maximum_values() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "supplier",
            "SELECT 1 AS suppkey, 'Alpha' AS name
             UNION ALL SELECT 2, 'Beta'
             UNION ALL SELECT 3, 'Gamma'",
        )
        .await?;
    session
        .register_sql_view(
            "lineitem",
            "SELECT 1 AS suppkey, 12 AS qty
             UNION ALL SELECT 1, 18
             UNION ALL SELECT 2, 15
             UNION ALL SELECT 3, 30",
        )
        .await?;

    let result = session
        .sql_scalar(
            "WITH revenue AS (
                 SELECT suppkey, SUM(qty) AS total FROM lineitem GROUP BY suppkey
             )
             SELECT COUNT(*) FROM supplier s
             JOIN revenue r ON s.suppkey = r.suppkey
             WHERE r.total = (SELECT MAX(total) FROM revenue)",
        )
        .await?;
    assert_eq!(result, ScalarValue::Int64(Some(2)));
    Ok(())
}

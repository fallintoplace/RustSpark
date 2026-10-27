use datafusion::{common::ScalarValue, functions_aggregate::expr_fn::sum, prelude::col};
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn groups_rows_lazily_with_typed_aggregates() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "orders",
            "SELECT 'Berlin' AS city, CAST(10 AS BIGINT) AS amount
             UNION ALL SELECT 'Berlin', 20
             UNION ALL SELECT 'Munich', 5",
        )
        .await?;

    let grouped = session
        .group_rows(
            "orders",
            &[col("city")],
            &[sum(col("amount")).alias("total")],
        )
        .await?;
    session.register_dataframe("totals", grouped)?;

    assert_eq!(
        session
            .sql_scalar("SELECT total FROM totals WHERE city = 'Berlin'")
            .await?,
        ScalarValue::Int64(Some(30))
    );
    assert_eq!(
        session
            .sql_scalar("SELECT total FROM totals WHERE city = 'Munich'")
            .await?,
        ScalarValue::Int64(Some(5))
    );

    let all_rows = session
        .group_rows("orders", &[], &[sum(col("amount")).alias("total")])
        .await?;
    session.register_dataframe("all_rows", all_rows)?;
    assert_eq!(
        session.sql_scalar("SELECT total FROM all_rows").await?,
        ScalarValue::Int64(Some(35))
    );

    assert!(
        session
            .group_rows("orders", &[col("city")], &[])
            .await
            .is_err()
    );
    assert!(
        session
            .group_rows("orders", &[col("unknown")], &[sum(col("amount"))])
            .await
            .is_err()
    );
    assert_eq!(session.table("orders").await?.count().await?, 3);
    Ok(())
}

use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn inequality_filter_handles_null_predicates() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "items",
            "SELECT CAST(1 AS BIGINT) AS n UNION ALL SELECT 2 UNION ALL SELECT CAST(NULL AS BIGINT)",
        )
        .await?;

    assert_eq!(
        session
            .filter_rows_not_equal_value("items", "n", ScalarValue::Int64(Some(1)))
            .await?
            .count()
            .await?,
        1
    );
    assert_eq!(
        session
            .filter_rows_not_equal_value("items", "n", ScalarValue::Int64(None))
            .await?
            .count()
            .await?,
        2
    );
    assert!(
        session
            .filter_rows_not_equal_value("items", "missing", ScalarValue::Int64(None))
            .await
            .is_err()
    );
    Ok(())
}

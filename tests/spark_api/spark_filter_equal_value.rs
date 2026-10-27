use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn equality_filter_matches_values_and_nulls() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "items",
            "SELECT CAST(NULL AS BIGINT) AS \"odd\"\"name\" UNION ALL SELECT 2 UNION ALL SELECT 2",
        )
        .await?;

    let two = session
        .filter_rows_equal_value("items", "odd\"name", ScalarValue::Int64(Some(2)))
        .await?;
    assert_eq!(two.count().await?, 2);
    let missing = session
        .filter_rows_equal_value("items", "odd\"name", ScalarValue::Int64(None))
        .await?;
    assert_eq!(missing.count().await?, 1);
    assert!(
        session
            .filter_rows_equal_value("items", "unknown", ScalarValue::Int64(Some(2)))
            .await
            .is_err()
    );
    Ok(())
}

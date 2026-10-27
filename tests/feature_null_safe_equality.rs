use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn null_safe_equality_matches_nulls_and_values() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "items",
            "SELECT 1 AS \"odd column\" UNION ALL
             SELECT CAST(NULL AS BIGINT) UNION ALL
             SELECT 2 UNION ALL SELECT CAST(NULL AS BIGINT)",
        )
        .await?;
    assert_eq!(
        session
            .filter_rows_null_safe_equal("items", "odd column", ScalarValue::Int64(None))
            .await?
            .count()
            .await?,
        2
    );
    assert_eq!(
        session
            .filter_rows_null_safe_equal("items", "odd column", ScalarValue::Int64(Some(1)))
            .await?
            .count()
            .await?,
        1
    );
    assert!(
        session
            .filter_rows_null_safe_equal("items", "missing", ScalarValue::Int64(None))
            .await
            .is_err()
    );
    Ok(())
}

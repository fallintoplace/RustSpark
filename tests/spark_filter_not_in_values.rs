use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn not_in_preserves_sql_null_semantics() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "items",
            "SELECT CAST(1 AS BIGINT) AS n UNION ALL SELECT 2 UNION ALL SELECT CAST(NULL AS BIGINT)",
        )
        .await?;

    let excluded = session
        .filter_rows_not_in_values("items", "n", &[ScalarValue::Int64(Some(1))])
        .await?;
    assert_eq!(excluded.count().await?, 1);

    let unknown = session
        .filter_rows_not_in_values(
            "items",
            "n",
            &[ScalarValue::Int64(Some(1)), ScalarValue::Int64(None)],
        )
        .await?;
    assert_eq!(unknown.count().await?, 0);

    assert_eq!(
        session
            .filter_rows_not_in_values("items", "n", &[])
            .await?
            .count()
            .await?,
        3
    );
    assert!(
        session
            .filter_rows_not_in_values("items", "missing", &[])
            .await
            .is_err()
    );
    Ok(())
}

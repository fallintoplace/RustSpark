use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn sql_union_distinct_deduplicates_rows_including_nulls() -> TestResult {
    let session = RustSparkSession::new();
    let combined = session
        .union_sql_distinct(&[
            "SELECT CAST(1 AS BIGINT) AS value",
            "SELECT CAST(1 AS BIGINT) AS value",
            "SELECT CAST(2 AS BIGINT) AS value",
            "SELECT CAST(NULL AS BIGINT) AS value",
            "SELECT CAST(NULL AS BIGINT) AS value",
        ])
        .await?;
    assert_eq!(combined.clone().count().await?, 3);

    session.register_dataframe("unique_values", combined)?;
    assert_eq!(
        session
            .sql_scalar("SELECT COUNT(value) FROM unique_values")
            .await?,
        ScalarValue::Int64(Some(2))
    );

    assert!(session.union_sql_distinct(&[]).await.is_err());
    assert!(
        session
            .union_sql_distinct(&["SELECT 1 AS a", "SELECT 1 AS a, 2 AS b"])
            .await
            .is_err()
    );
    Ok(())
}

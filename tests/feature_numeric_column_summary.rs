use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn summary_counts_nulls_and_computes_numeric_means() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "metrics",
            "SELECT 1 AS \"score total\" UNION ALL
             SELECT 3 UNION ALL SELECT CAST(NULL AS BIGINT)",
        )
        .await?;
    let summary = session.numeric_column_summary("metrics", "score total").await?;
    session.register_dataframe("stats", summary)?;
    assert_eq!(session.sql_scalar("SELECT row_count FROM stats").await?.to_string(), "3");
    assert_eq!(session.sql_scalar("SELECT non_null_count FROM stats").await?.to_string(), "2");
    assert_eq!(session.sql_scalar("SELECT null_count FROM stats").await?.to_string(), "1");
    assert_eq!(session.sql_scalar("SELECT min_value FROM stats").await?.to_string(), "1");
    assert_eq!(session.sql_scalar("SELECT max_value FROM stats").await?.to_string(), "3");
    assert_eq!(
        session.sql_scalar("SELECT CAST(mean_value AS BIGINT) FROM stats").await?.to_string(),
        "2"
    );
    assert!(session.numeric_column_summary("metrics", "missing").await.is_err());
    Ok(())
}

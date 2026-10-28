use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn grouped_numeric_summary_aggregates_without_collect() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT 'a' AS segment, 1 AS metric, 2 AS weight UNION ALL
             SELECT 'a', 3, 1 UNION ALL SELECT 'b', CAST(NULL AS INT), 4
             UNION ALL SELECT 'b', 5, 1",
        )
        .await?;
    let result = session.numeric_summary_per_group("events", "segment", "metric").await?;
    session.register_dataframe("analytics", result)?;
    assert_eq!(
        session.sql_scalar("SELECT SUM(non_null_count) FROM analytics").await?.to_string(),
        "3"
    );
    assert!(session.numeric_summary_per_group("events", "segment", "missing").await.is_err());
    Ok(())
}

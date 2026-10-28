use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn grouped_min_max_reports_group_aggregates() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT 'a' AS segment, 1 AS metric UNION ALL SELECT 'a', 3 UNION ALL
             SELECT 'b', CAST(NULL AS INT) UNION ALL SELECT 'b', 5",
        )
        .await?;
    let result = session.min_max_per_group("events", "segment", "metric").await?;
    session.register_dataframe("analytics", result)?;
    assert_eq!(
        session.sql_scalar("SELECT COUNT(*) FROM analytics WHERE max_value >= min_value").await?.to_string(),
        "2"
    );
    assert!(session.min_max_per_group("events", "segment", "missing").await.is_err());
    Ok(())
}

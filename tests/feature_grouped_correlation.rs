use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn grouped_correlation_handles_groups_with_missing_pairs() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT 'a' AS segment, 1 AS metric, 2 AS weight UNION ALL
             SELECT 'a', 3, 1 UNION ALL SELECT 'b', CAST(NULL AS INT), 4
             UNION ALL SELECT 'b', 5, 1",
        )
        .await?;
    let result = session.correlation_per_group("events", "segment", "metric", "weight").await?;
    session.register_dataframe("analytics", result)?;
    assert_eq!(
        session
            .sql_scalar("SELECT COUNT(*) FROM analytics WHERE correlation IS NOT NULL")
            .await?
            .to_string(),
        "1"
    );
    assert!(session.correlation_per_group("events", "segment", "missing", "weight").await.is_err());
    Ok(())
}

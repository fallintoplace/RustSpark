use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn grouped_weighted_mean_aggregates_without_collect() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT 'a' AS segment, 1 AS metric, 2 AS weight UNION ALL
             SELECT 'a', 3, 1 UNION ALL SELECT 'b', CAST(NULL AS INT), 4
             UNION ALL SELECT 'b', 5, 1",
        )
        .await?;
    let result = session
        .weighted_mean_per_group("events", "segment", "metric", "weight")
        .await?;
    session.register_dataframe("analytics", result)?;
    assert_eq!(
        session
            .sql_scalar("SELECT COUNT(*) FROM analytics WHERE weighted_mean > 1")
            .await?
            .to_string(),
        "2"
    );
    assert!(
        session
            .weighted_mean_per_group("events", "segment", "missing", "weight")
            .await
            .is_err()
    );
    Ok(())
}

use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn grouped_approx_percentile_returns_grouped_results() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT 'a' AS segment, 1 AS metric UNION ALL SELECT 'a', 3 UNION ALL
             SELECT 'b', CAST(NULL AS INT) UNION ALL SELECT 'b', 5",
        )
        .await?;
    let result = session
        .approx_percentile_per_group("events", "segment", "metric", 0.5)
        .await?;
    session.register_dataframe("analytics", result)?;
    assert_eq!(
        session
            .sql_scalar("SELECT COUNT(*) FROM analytics WHERE estimated_percentile IS NOT NULL")
            .await?
            .to_string(),
        "2"
    );
    assert!(
        session
            .approx_percentile_per_group("events", "segment", "missing", 0.5)
            .await
            .is_err()
    );
    Ok(())
}

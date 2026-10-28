use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn grouped_stddev_runs_grouped_native_aggregate() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT 'a' AS segment, 1 AS metric UNION ALL SELECT 'a', 3 UNION ALL
             SELECT 'b', CAST(NULL AS INT) UNION ALL SELECT 'b', 5",
        )
        .await?;
    let result = session
        .stddev_per_group("events", "segment", "metric")
        .await?;
    session.register_dataframe("analytics", result)?;
    assert_eq!(
        session
            .sql_scalar("SELECT COUNT(*) FROM analytics WHERE sample_stddev IS NOT NULL")
            .await?
            .to_string(),
        "1"
    );
    assert!(
        session
            .stddev_per_group("events", "missing", "metric")
            .await
            .is_err()
    );
    Ok(())
}

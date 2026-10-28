use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn fixed_width_histogram_stays_in_engine_and_checks_inputs() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT 1 AS metric UNION ALL SELECT 3 UNION ALL SELECT 5 UNION ALL SELECT CAST(NULL AS INT)",
        )
        .await?;
    let result = session.fixed_width_histogram("events", "metric", 2.0).await?;
    session.register_dataframe("analytics", result)?;
    assert_eq!(
        session.sql_scalar("SELECT SUM(row_count) FROM analytics").await?.to_string(),
        "3"
    );
    assert!(session.fixed_width_histogram("events", "missing", 2.0).await.is_err());
    assert!(session.fixed_width_histogram("events", "metric", 0.0).await.is_err());
    assert!(session.fixed_width_histogram("events", "metric", f64::NAN).await.is_err());
    Ok(())
}

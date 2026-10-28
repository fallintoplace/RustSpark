use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn approx_duplicate_count_stays_in_engine_and_checks_inputs() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT 1 AS metric UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT CAST(NULL AS INT)",
        )
        .await?;
    let result = session
        .approximate_duplicate_count("events", "metric")
        .await?;
    session.register_dataframe("analytics", result)?;
    assert_eq!(
        session
            .sql_scalar("SELECT non_null_rows FROM analytics")
            .await?
            .to_string(),
        "4"
    );
    assert!(
        session
            .approximate_duplicate_count("events", "missing")
            .await
            .is_err()
    );
    Ok(())
}

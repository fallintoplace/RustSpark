use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn approx_quantile_summary_stays_in_engine_and_checks_inputs() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT 1 AS metric UNION ALL SELECT 3 UNION ALL SELECT 5 UNION ALL SELECT CAST(NULL AS INT)",
        )
        .await?;
    let result = session
        .approximate_quantile_summary("events", "metric")
        .await?;
    session.register_dataframe("analytics", result)?;
    assert_eq!(
        session
            .sql_scalar("SELECT COUNT(*) FROM analytics WHERE q25 IS NOT NULL AND q50 IS NOT NULL AND q75 IS NOT NULL")
            .await?
            .to_string(),
        "1"
    );
    assert!(
        session
            .approximate_quantile_summary("events", "missing")
            .await
            .is_err()
    );
    Ok(())
}

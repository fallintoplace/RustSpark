use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn approximates_numeric_percentiles_and_checks_bounds() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "metrics",
            "SELECT 7 AS \"score x\" UNION ALL SELECT 7 UNION ALL
             SELECT 7 UNION ALL SELECT CAST(NULL AS BIGINT)",
        )
        .await?;
    let estimate = session.approx_column_percentile("metrics", "score x", 0.5).await?;
    session.register_dataframe("estimate", estimate)?;
    assert_eq!(
        session.sql_scalar("SELECT CAST(estimated_percentile AS BIGINT) FROM estimate").await?.to_string(),
        "7"
    );
    assert!(session.approx_column_percentile("metrics", "score x", -0.1).await.is_err());
    assert!(session.approx_column_percentile("metrics", "score x", f64::NAN).await.is_err());
    assert!(session.approx_column_percentile("metrics", "score x", f64::INFINITY).await.is_err());
    assert!(session.approx_column_percentile("metrics", "missing", 0.5).await.is_err());
    Ok(())
}

use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn grouped_completeness_reports_group_aggregates() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT 'a' AS segment, 1 AS metric UNION ALL SELECT 'a', 3 UNION ALL
             SELECT 'b', CAST(NULL AS INT) UNION ALL SELECT 'b', 5",
        )
        .await?;
    let result = session.non_null_ratio_per_group("events", "segment", "metric").await?;
    session.register_dataframe("analytics", result)?;
    assert_eq!(
        session.sql_scalar("SELECT COUNT(*) FROM analytics WHERE completeness > 0.5").await?.to_string(),
        "1"
    );
    assert!(session.non_null_ratio_per_group("events", "segment", "missing").await.is_err());
    Ok(())
}

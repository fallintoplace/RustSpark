use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn top_groups_by_sum_runs_grouped_native_aggregate() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT 'a' AS segment, 1 AS metric UNION ALL SELECT 'a', 3 UNION ALL
             SELECT 'b', CAST(NULL AS INT) UNION ALL SELECT 'b', 5",
        )
        .await?;
    let result = session.top_groups_by_sum("events", "segment", "metric", 1).await?;
    session.register_dataframe("analytics", result)?;
    assert_eq!(
        session
            .sql_scalar("SELECT COUNT(*) FROM analytics WHERE CAST(total AS BIGINT) = 5")
            .await?
            .to_string(),
        "1"
    );
    assert!(session.top_groups_by_sum("events", "missing", "metric", 1).await.is_err());
    Ok(())
}

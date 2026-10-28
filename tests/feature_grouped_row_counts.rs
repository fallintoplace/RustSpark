use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn grouped_row_counts_reports_group_aggregates() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT 'a' AS segment, 1 AS metric UNION ALL SELECT 'a', 3 UNION ALL
             SELECT 'b', CAST(NULL AS INT) UNION ALL SELECT 'b', 5",
        )
        .await?;
    let result = session
        .row_counts_per_group("events", "segment", "metric")
        .await?;
    session.register_dataframe("analytics", result)?;
    assert_eq!(
        session
            .sql_scalar("SELECT SUM(row_count) FROM analytics")
            .await?
            .to_string(),
        "4"
    );
    assert!(
        session
            .row_counts_per_group("events", "segment", "missing")
            .await
            .is_err()
    );
    Ok(())
}

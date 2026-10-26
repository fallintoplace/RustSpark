use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn cross_join_produces_cartesian_product() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "order",
            "SELECT CAST(1 AS BIGINT) AS left_id UNION ALL SELECT 2",
        )
        .await?;
    session
        .register_sql_view(
            "lookup",
            "SELECT 'a' AS right_id UNION ALL SELECT 'b' UNION ALL SELECT 'c'",
        )
        .await?;

    let rows = session.cross_join_tables("order", "lookup").await?;
    assert_eq!(rows.count().await?, 6);
    assert_eq!(session.table("order").await?.count().await?, 2);
    assert_eq!(session.table("lookup").await?.count().await?, 3);
    assert!(
        session
            .cross_join_tables("order", "unknown")
            .await
            .is_err()
    );
    assert!(
        session
            .cross_join_tables("order; DROP TABLE lookup", "lookup")
            .await
            .is_err()
    );
    Ok(())
}

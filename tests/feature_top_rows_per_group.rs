use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn grouped_top_rows_keep_two_per_partition() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "scores",
            "SELECT 'a' AS \"group name\", 1 AS id, 7 AS score UNION ALL
             SELECT 'a', 2, 9 UNION ALL SELECT 'a', 3, 8 UNION ALL
             SELECT 'b', 4, 2 UNION ALL SELECT 'b', 5, 4",
        )
        .await?;
    let top = session
        .top_rows_per_group("scores", &["group name"], &[("score", false), ("id", true)], 2)
        .await?;
    assert_eq!(top.schema().fields().len(), 3);
    session.register_dataframe("best", top)?;
    assert_eq!(session.sql_scalar("SELECT COUNT(*) FROM best").await?.to_string(), "4");
    assert_eq!(
        session.sql_scalar("SELECT COUNT(*) FROM best WHERE id IN (2, 3, 4, 5)").await?.to_string(),
        "4"
    );
    assert_eq!(
        session.top_rows_per_group("scores", &["group name"], &[("score", false)], 0).await?.count().await?,
        0
    );
    assert!(session.top_rows_per_group("scores", &[], &[("score", true)], 1).await.is_err());
    assert!(session.top_rows_per_group("scores", &["group name"], &[("missing", true)], 1).await.is_err());
    Ok(())
}

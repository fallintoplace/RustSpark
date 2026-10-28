use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn summarizes_partition_skew_without_collecting_group_rows() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT 'a' AS segment, 1 AS id UNION ALL SELECT 'a', 2 UNION ALL SELECT 'b', 3",
        )
        .await?;
    let result = session.group_size_skew("events", "segment").await?;
    session.register_dataframe("skew", result)?;
    assert_eq!(
        session
            .sql_scalar("SELECT group_count FROM skew")
            .await?
            .to_string(),
        "2"
    );
    assert_eq!(
        session
            .sql_scalar("SELECT min_rows FROM skew")
            .await?
            .to_string(),
        "1"
    );
    assert_eq!(
        session
            .sql_scalar("SELECT max_rows FROM skew")
            .await?
            .to_string(),
        "2"
    );
    assert!(session.group_size_skew("events", "missing").await.is_err());
    Ok(())
}

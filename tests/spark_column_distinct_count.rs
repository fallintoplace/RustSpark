use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn distinct_count_can_include_or_exclude_nulls() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "items",
            "SELECT 'apple' AS label UNION ALL SELECT 'apple'
             UNION ALL SELECT 'pear' UNION ALL SELECT CAST(NULL AS VARCHAR)
             UNION ALL SELECT CAST(NULL AS VARCHAR)",
        )
        .await?;

    assert_eq!(session.column_distinct_count("items", "label", false).await?, 2);
    assert_eq!(session.column_distinct_count("items", "label", true).await?, 3);
    assert_eq!(session.table("items").await?.count().await?, 5);
    assert!(
        session
            .column_distinct_count("items", "missing", true)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn distinct_count_handles_empty_tables() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view("empty", "SELECT 1 AS n WHERE FALSE")
        .await?;
    assert_eq!(session.column_distinct_count("empty", "n", true).await?, 0);
    assert_eq!(session.column_distinct_count("empty", "n", false).await?, 0);
    Ok(())
}

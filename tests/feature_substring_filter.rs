use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn substring_filter_handles_case_null_and_sql_metacharacters() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "words",
            "SELECT 'hello' AS \"phrase text\" UNION ALL
             SELECT 'HELLO' UNION ALL SELECT CAST(NULL AS VARCHAR)",
        )
        .await?;
    assert_eq!(session.filter_rows_contains("words", "phrase text", "ell", true).await?.count().await?, 1);
    assert_eq!(session.filter_rows_contains("words", "phrase text", "ell", false).await?.count().await?, 2);
    assert_eq!(session.filter_rows_contains("words", "phrase text", "' OR 1=1 --", false).await?.count().await?, 0);
    assert!(session.filter_rows_contains("words", "missing", "ell", true).await.is_err());
    Ok(())
}

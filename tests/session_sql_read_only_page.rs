use rustspark::RustSparkSession;

#[tokio::test]
async fn pages_safe_sql_and_checks_bounds() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let query = "SELECT value FROM (VALUES (1), (2), (3)) AS t(value) ORDER BY value";
    assert_eq!(session.sql_read_only_page(query, 1, 1).await?.count().await?, 1);
    assert_eq!(session.sql_read_only_page(query, 3, 1).await?.count().await?, 0);
    assert!(session.sql_read_only_page(query, 0, 0).await.is_err());
    assert!(
        session
            .sql_read_only_page(query, usize::MAX, 2)
            .await
            .is_err()
    );
    assert!(
        session
            .sql_read_only_page("DROP TABLE data", 0, 1)
            .await
            .is_err()
    );
    Ok(())
}

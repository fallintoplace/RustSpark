use rustspark::RustSparkSession;

#[tokio::test]
async fn cached_table_does_not_reopen_the_source_file() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let csv = dir.path().join("input.csv");
    std::fs::write(&csv, "value\n1\n2\n3\n")?;

    let session = RustSparkSession::new();
    session.register_csv("items", &csv).await?;
    session.cache_table("items").await?;

    std::fs::remove_file(&csv)?;

    assert_eq!(session.table("items").await?.count().await?, 3);
    assert_eq!(
        session
            .sql("SELECT * FROM items WHERE value >= 2")
            .await?
            .count()
            .await?,
        2
    );
    Ok(())
}

#[tokio::test]
async fn cannot_cache_a_missing_table() {
    let session = RustSparkSession::new();
    assert!(session.cache_table("missing").await.is_err());
}

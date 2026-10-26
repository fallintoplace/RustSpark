use rustspark::RustSparkSession;

#[tokio::test]
async fn renames_without_copying_or_overwriting() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session.register_csv("sales", concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")).await?;
    session.rename_table("sales", "transactions").await?;
    assert!(session.table("sales").await.is_err());
    assert_eq!(session.table("transactions").await?.count().await?, 3);

    session.register_csv("existing", concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")).await?;
    assert!(session.rename_table("transactions", "existing").await.is_err());
    assert!(session.rename_table("existing", "existing").await.is_err());
    assert!(session.rename_table("missing", "another").await.is_err());
    assert_eq!(session.table("transactions").await?.count().await?, 3);
    assert_eq!(session.table("existing").await?.count().await?, 3);
    Ok(())
}

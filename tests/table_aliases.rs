use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn aliases_share_table_data_without_requiring_source_registration() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_csv(
            "sales",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
        )
        .await?;

    session.register_table_alias("sales_copy", "sales").await?;
    assert!(session.deregister_table("sales")?);
    assert_eq!(session.table("sales_copy").await?.count().await?, 3);
    assert!(session.table("sales").await.is_err());

    session.register_table_alias("other_copy", "sales_copy").await?;
    assert_eq!(session.table("other_copy").await?.count().await?, 3);
    Ok(())
}

#[tokio::test]
async fn missing_sources_and_duplicate_aliases_are_rejected() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view("base", "SELECT 17 AS value")
        .await?;

    assert!(session.register_table_alias("base", "base").await.is_err());
    assert!(
        session
            .register_table_alias("other", "nonexistent")
            .await
            .is_err()
    );
    assert!(session.table("other").await.is_err());
    assert_eq!(session.table("base").await?.count().await?, 1);
    Ok(())
}

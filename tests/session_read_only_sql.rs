use rustspark::RustSparkSession;

#[tokio::test]
async fn accepts_select_but_blocks_schema_mutations() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_csv(
            "sales",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
        )
        .await?;

    assert_eq!(
        session
            .sql_read_only("SELECT * FROM sales")
            .await?
            .count()
            .await?,
        3
    );
    assert!(
        session
            .sql_read_only("CREATE TABLE blocked (id INT)")
            .await
            .is_err()
    );
    assert!(session.sql_read_only("DROP TABLE sales").await.is_err());
    assert!(session.table("blocked").await.is_err());
    assert_eq!(session.table("sales").await?.count().await?, 3);
    Ok(())
}

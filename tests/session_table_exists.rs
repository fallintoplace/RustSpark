use rustspark::RustSparkSession;

#[tokio::test]
async fn reports_table_registration_and_removal() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    assert!(!session.table_exists("sales")?);
    session
        .register_csv(
            "sales",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
        )
        .await?;
    assert!(session.table_exists("sales")?);
    assert!(session.deregister_table("sales")?);
    assert!(!session.table_exists("sales")?);
    Ok(())
}

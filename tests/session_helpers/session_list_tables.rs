use rustspark::RustSparkSession;

#[tokio::test]
async fn enumerates_registered_tables_in_sorted_order() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_csv(
            "sales",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
        )
        .await?;
    session.register_table_alias("copy", "sales").await?;
    let tables = session.list_tables();
    assert!(
        tables.iter().any(|name| name.ends_with(".sales")),
        "{tables:?}"
    );
    assert!(
        tables.iter().any(|name| name.ends_with(".copy")),
        "{tables:?}"
    );
    assert!(tables.windows(2).all(|pair| pair[0] <= pair[1]));
    session.deregister_table("copy")?;
    assert!(
        !session
            .list_tables()
            .iter()
            .any(|name| name.ends_with(".copy"))
    );
    Ok(())
}

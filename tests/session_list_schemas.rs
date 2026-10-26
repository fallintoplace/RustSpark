use rustspark::RustSparkSession;

#[tokio::test]
async fn lists_available_schemas_in_stable_order() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_csv(
            "sales",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
        )
        .await?;

    let schemas = session.list_schemas();
    assert!(!schemas.is_empty());
    let mut sorted = schemas.clone();
    sorted.sort();
    assert_eq!(schemas, sorted);
    let table = session
        .list_tables()
        .into_iter()
        .find(|name| name.ends_with(".sales"))
        .expect("sales must be registered");
    let schema = table.rsplit_once('.').expect("qualified table name").0;
    assert!(schemas.iter().any(|name| name == schema));
    Ok(())
}

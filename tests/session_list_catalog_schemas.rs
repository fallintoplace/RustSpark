use rustspark::RustSparkSession;

#[test]
fn lists_schemas_within_one_catalog_and_rejects_missing_catalogs()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let schemas = session.list_schemas_in_catalog("datafusion")?;
    assert!(schemas.iter().any(|name| name == "public"));
    let mut sorted = schemas.clone();
    sorted.sort();
    assert_eq!(schemas, sorted);
    assert!(session.list_schemas_in_catalog("missing").is_err());
    Ok(())
}

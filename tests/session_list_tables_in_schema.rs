use rustspark::RustSparkSession;

#[tokio::test]
async fn lists_tables_within_a_catalog_schema() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session.register_sql_view("beta", "SELECT 2 AS id").await?;
    session.register_sql_view("alpha", "SELECT 1 AS id").await?;

    let qualified = session
        .list_tables()
        .into_iter()
        .find(|name| name.ends_with(".alpha"))
        .expect("registered table");
    let (catalog_schema, _) = qualified.rsplit_once('.').unwrap();
    let (catalog, schema) = catalog_schema.rsplit_once('.').unwrap();

    let tables = session.list_tables_in_schema(catalog, schema)?;
    assert!(tables.contains(&"alpha".to_string()));
    assert!(tables.contains(&"beta".to_string()));
    assert!(tables.windows(2).all(|pair| pair[0] <= pair[1]));
    assert!(
        session
            .list_tables_in_schema("missing_catalog", schema)
            .is_err()
    );
    assert!(
        session
            .list_tables_in_schema(catalog, "missing_schema")
            .is_err()
    );
    Ok(())
}

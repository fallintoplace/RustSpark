use rustspark::RustSparkSession;

#[test]
fn lists_catalogs_in_stable_order() {
    let session = RustSparkSession::new();
    let catalogs = session.list_catalogs();
    assert!(!catalogs.is_empty());
    assert!(catalogs.windows(2).all(|pair| pair[0] <= pair[1]));
    for qualified_schema in session.list_schemas() {
        assert!(
            catalogs
                .iter()
                .any(|catalog| qualified_schema.starts_with(&format!("{catalog}."))),
            "{qualified_schema} is missing from {catalogs:?}"
        );
    }
}

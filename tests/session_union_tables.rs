use rustspark::RustSparkSession;

#[tokio::test]
async fn combines_same_schema_tables_and_preserves_duplicates() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session.register_csv("sales", concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")).await?;
    session.register_sql_view("berlin", "SELECT city, amount FROM sales WHERE city = 'Berlin'").await?;
    session.register_sql_view("munich", "SELECT city, amount FROM sales WHERE city = 'Munich'").await?;
    let combined = session.union_tables(&["berlin", "munich"]).await?;
    assert_eq!(combined.count().await?, 3);
    assert_eq!(session.union_tables(&["sales", "sales"]).await?.count().await?, 6);
    session.register_sql_view("just_city", "SELECT city FROM sales").await?;
    assert!(session.union_tables(&["sales", "just_city"]).await.is_err());
    assert!(session.union_tables(&[]).await.is_err());
    Ok(())
}

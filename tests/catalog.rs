use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn sales_csv() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")
}

#[tokio::test]
async fn removes_tables_and_allows_the_name_to_be_reused() -> TestResult {
    let session = RustSparkSession::new();
    session.register_csv("sales", sales_csv()).await?;
    assert!(session.deregister_table("sales")?);
    assert!(!session.deregister_table("sales")?);
    assert!(session.sql("SELECT * FROM sales").await.is_err());

    session.register_csv("sales", sales_csv()).await?;
    assert_eq!(session.table("sales").await?.count().await?, 3);
    Ok(())
}

#[tokio::test]
async fn removing_a_view_does_not_modify_the_source_table() -> TestResult {
    let session = RustSparkSession::new();
    session.register_csv("sales", sales_csv()).await?;
    let frame = session.table("sales").await?;
    session.register_dataframe("sales_view", frame)?;
    assert!(session.deregister_table("sales_view")?);
    assert_eq!(session.table("sales").await?.count().await?, 3);
    assert!(session.table("sales_view").await.is_err());
    Ok(())
}

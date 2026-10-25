use datafusion::prelude::{col, lit};
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn sales_csv() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")
}

#[tokio::test]
async fn reads_registered_sql_tables_as_lazy_dataframes() -> TestResult {
    let session = RustSparkSession::new();
    session.register_csv("sales", sales_csv()).await?;

    let frame = session.table("sales").await?;
    assert_eq!(frame.filter(col("amount").gt(lit(5)))?.count().await?, 2);
    assert!(session.table("missing").await.is_err());
    Ok(())
}

#[tokio::test]
async fn reads_registered_dataframe_views_as_dataframes() -> TestResult {
    let session = RustSparkSession::new();
    let source = session.read_csv(sales_csv()).await?;
    session.register_dataframe("filtered", source.filter(col("amount").gt(lit(9)))?)?;
    assert_eq!(session.table("filtered").await?.count().await?, 2);
    Ok(())
}

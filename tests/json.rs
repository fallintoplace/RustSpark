use datafusion::{
    arrow::array::Int64Array,
    prelude::{JsonReadOptions, col, lit},
};
use rustspark::RustSparkSession;

fn sales_json() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.json")
}

#[tokio::test]
async fn queries_registered_json() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session.register_json("sales", sales_json()).await?;

    let batches = session
        .sql("SELECT SUM(amount) AS total FROM sales")
        .await?
        .collect()
        .await?;
    let totals = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(totals.value(0), 35);
    Ok(())
}

#[tokio::test]
async fn reads_json_lazily_with_options() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let filtered = session
        .read_json_with_options(
            sales_json(),
            JsonReadOptions::default().schema_infer_max_records(10),
        )
        .await?
        .filter(col("amount").gt(lit(9)))?;
    assert_eq!(filtered.count().await?, 2);
    assert!(session.sql("SELECT * FROM sales").await.is_err());
    Ok(())
}

#[tokio::test]
async fn rejects_missing_json_files() {
    let result = RustSparkSession::new()
        .read_json("tests/fixtures/does_not_exist.json")
        .await;
    assert!(result.is_err());
}

use datafusion::{
    arrow::array::Int64Array,
    prelude::{col, lit},
};
use rustspark::RustSparkSession;

fn sales_csv() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")
}

#[tokio::test]
async fn exposes_filtered_dataframe_to_sql() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let frame = session
        .read_csv(sales_csv())
        .await?
        .filter(col("amount").gt(lit(5)))?
        .select_columns(&["amount"])?;
    session.register_dataframe("filtered_sales", frame)?;

    let batches = session
        .sql("SELECT SUM(amount) AS total FROM filtered_sales")
        .await?
        .collect()
        .await?;
    assert_eq!(batches.len(), 1);
    let totals = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(totals.value(0), 30);
    assert!(session
        .sql("SELECT city FROM filtered_sales")
        .await
        .is_err());
    assert!(RustSparkSession::new()
        .sql("SELECT * FROM filtered_sales")
        .await
        .is_err());
    Ok(())
}

#[tokio::test]
async fn rejects_duplicate_dataframe_name() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session.register_csv("sales", sales_csv()).await?;
    let frame = session
        .read_csv(sales_csv())
        .await?
        .filter(col("amount").gt(lit(5)))?;

    assert!(session.register_dataframe("sales", frame).is_err());
    assert_eq!(session.sql("SELECT * FROM sales").await?.count().await?, 3);
    Ok(())
}

#[tokio::test]
async fn rejects_dataframe_from_another_session() -> Result<(), Box<dyn std::error::Error>> {
    let source = RustSparkSession::new();
    let destination = RustSparkSession::new();
    let frame = source.read_csv(sales_csv()).await?;

    assert!(destination.register_dataframe("sales", frame).is_err());
    assert!(destination.sql("SELECT * FROM sales").await.is_err());
    Ok(())
}

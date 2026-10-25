use datafusion::arrow::array::{Int64Array, StringArray};
use rustspark::RustSparkSession;

fn sales_csv() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")
}

#[tokio::test]
async fn aggregates_csv_rows() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session.register_csv("sales", sales_csv()).await?;

    let batches = session
        .sql("SELECT city, SUM(amount) AS total FROM sales GROUP BY city ORDER BY city")
        .await?
        .collect()
        .await?;

    let mut rows = Vec::new();
    for batch in batches {
        let cities = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let totals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((cities.value(index).to_owned(), totals.value(index)));
        }
    }

    assert_eq!(
        rows,
        vec![("Berlin".to_owned(), 30), ("Munich".to_owned(), 5)]
    );
    Ok(())
}

#[tokio::test]
async fn unknown_table_returns_an_error() {
    let session = RustSparkSession::new();
    assert!(session.sql("SELECT * FROM missing").await.is_err());
}

#[tokio::test]
async fn filters_csv_rows() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session.register_csv("sales", sales_csv()).await?;

    let batches = session
        .sql("SELECT COUNT(*) FROM sales WHERE amount > 5")
        .await?
        .collect()
        .await?;

    assert_eq!(batches.len(), 1);
    let counts = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.value(0), 2);
    Ok(())
}

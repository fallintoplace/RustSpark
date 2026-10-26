use datafusion::arrow::array::Int64Array;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn unions_different_column_orders_and_missing_columns() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_csv(
            "sales",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
        )
        .await?;
    session.register_sql_view(
        "berlin",
        "SELECT city, amount FROM sales WHERE city = 'Berlin'",
    ).await?;
    session.register_sql_view(
        "munich",
        "SELECT amount, city, 1 AS source FROM sales WHERE city = 'Munich'",
    ).await?;
    let union = session.union_tables_by_name(&["berlin", "munich"]).await?;
    session.register_dataframe("combined", union)?;

    let batches = session.sql(
        "SELECT COUNT(*) AS rows, COUNT(source) AS tagged, SUM(amount) AS total FROM combined",
    ).await?.collect().await?;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    for (column, expected) in [(0, 3), (1, 1), (2, 35)] {
        let values = batch.column(column).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.value(0), expected);
    }
    Ok(())
}

#[tokio::test]
async fn rejects_empty_and_unknown_input_tables() -> TestResult {
    let session = RustSparkSession::new();
    assert!(session.union_tables_by_name(&[]).await.is_err());
    assert!(session.union_tables_by_name(&["missing"]).await.is_err());
    Ok(())
}

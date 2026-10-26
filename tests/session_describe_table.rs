use datafusion::arrow::datatypes::DataType;
use rustspark::RustSparkSession;

#[tokio::test]
async fn describes_registered_table_without_running_a_query()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_csv(
            "sales",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
        )
        .await?;
    let columns = session.describe_table("sales").await?;
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].0, "city");
    assert_eq!(columns[0].1, DataType::Utf8);
    assert_eq!(columns[1].0, "amount");
    assert_eq!(columns[1].1, DataType::Int64);
    assert!(session.describe_table("missing").await.is_err());
    Ok(())
}

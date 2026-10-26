use datafusion::arrow::datatypes::DataType;
use rustspark::RustSparkSession;

#[tokio::test]
async fn describes_query_columns_without_executing_mutations()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let columns = session
        .describe_sql("SELECT CAST(1 AS BIGINT) AS id, CAST(NULL AS VARCHAR) AS label")
        .await?;
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].0, "id");
    assert_eq!(columns[0].1, DataType::Int64);
    assert_eq!(columns[1].0, "label");
    assert_eq!(columns[1].1, DataType::Utf8View);
    assert!(
        session
            .describe_sql("CREATE TABLE unsafe (value INT)")
            .await
            .is_err()
    );
    assert!(session.table("unsafe").await.is_err());
    Ok(())
}

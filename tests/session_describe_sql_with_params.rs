use datafusion::{arrow::datatypes::DataType, common::ScalarValue};
use rustspark::RustSparkSession;

#[tokio::test]
async fn inspects_bound_sql_without_running_it() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let columns = session
        .describe_sql_with_params(
            "SELECT CAST($1 AS BIGINT) AS answer",
            vec![ScalarValue::Int64(Some(42))],
        )
        .await?;
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].0, "answer");
    assert_eq!(columns[0].1, DataType::Int64);
    assert!(
        session
            .describe_sql_with_params("DROP TABLE data", Vec::<ScalarValue>::new())
            .await
            .is_err()
    );
    Ok(())
}

use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn fetches_first_bound_row_with_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let row = session
        .sql_first_row_with_params(
            "SELECT CAST($1 AS BIGINT), CAST(NULL AS BIGINT)",
            vec![ScalarValue::Int64(Some(9))],
        )
        .await?;
    assert_eq!(
        row,
        Some(vec![ScalarValue::Int64(Some(9)), ScalarValue::Int64(None)])
    );

    let empty = session
        .sql_first_row_with_params(
            "SELECT CAST($1 AS BIGINT) WHERE FALSE",
            vec![ScalarValue::Int64(Some(9))],
        )
        .await?;
    assert_eq!(empty, None);
    Ok(())
}

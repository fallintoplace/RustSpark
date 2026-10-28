use datafusion::arrow::array::Int64Array;
use rustspark::RustSparkSession;

#[tokio::test]
async fn shiftleft_with_scalar_rhs_preserves_rows() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session.sql(
        "SELECT SHIFTLEFT(n, CAST(1 AS BIGINT)) FROM (VALUES (0, CAST(5 AS BIGINT)), (1, CAST(-1 AS BIGINT)), (2, CAST(NULL AS BIGINT))) AS t(idx, n) ORDER BY idx",
    ).await?.collect().await?;
    let mut values = Vec::new();
    for batch in &batches {
        values.extend(batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().iter());
    }
    assert_eq!(values, vec![Some(10), Some(-2), None]);
    Ok(())
}

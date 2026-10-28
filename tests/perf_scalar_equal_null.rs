use datafusion::arrow::array::BooleanArray;
use rustspark::RustSparkSession;

#[tokio::test]
async fn equal_null_with_scalar_rhs_preserves_rows() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session.sql(
        "SELECT EQUAL_NULL(n, CAST(7 AS BIGINT)) FROM (VALUES (0, CAST(7 AS BIGINT)), (1, CAST(2 AS BIGINT)), (2, CAST(NULL AS BIGINT))) AS t(idx, n) ORDER BY idx",
    ).await?.collect().await?;
    let mut values = Vec::new();
    for batch in &batches {
        values.extend(batch.column(0).as_any().downcast_ref::<BooleanArray>().unwrap().iter());
    }
    assert_eq!(values, vec![Some(true), Some(false), Some(false)]);
    Ok(())
}

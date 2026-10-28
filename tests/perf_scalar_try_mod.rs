use datafusion::arrow::array::Int64Array;
use rustspark::RustSparkSession;

#[tokio::test]
async fn try_mod_scalar_operand_keeps_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session.sql(
        "SELECT TRY_MOD(n, CAST(3 AS BIGINT)) AS value FROM \\
         (VALUES (0, CAST(7 AS BIGINT)), (1, CAST(NULL AS BIGINT))) AS t(idx, n) ORDER BY idx",
    ).await?.collect().await?;
    let mut values = Vec::new();
    for batch in &batches {
        let column = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        values.extend(column.iter());
    }
    assert_eq!(values, vec![Some(1), None]);
    Ok(())
}

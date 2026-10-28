use datafusion::arrow::array::Int64Array;
use rustspark::RustSparkSession;

#[tokio::test]
async fn zeroifnull_keeps_dense_and_nullable_values() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session.sql(
        "SELECT ZEROIFNULL(n) FROM (VALUES (0, CAST(5 AS BIGINT)), (1, CAST(0 AS BIGINT)), (2, CAST(-1 AS BIGINT))) AS t(idx, n) ORDER BY idx",
    ).await?.collect().await?;
    let mut values = Vec::new();
    for batch in &batches {
        values.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter(),
        );
    }
    assert_eq!(values, vec![Some(5), Some(0), Some(-1)]);
    let null_batches = session.sql(
        "SELECT ZEROIFNULL(n) FROM (VALUES (0, CAST(NULL AS BIGINT)), (1, CAST(3 AS BIGINT))) AS t(idx, n) ORDER BY idx",
    ).await?.collect().await?;
    let mut null_values = Vec::new();
    for batch in &null_batches {
        null_values.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter(),
        );
    }
    assert_eq!(null_values, vec![Some(0), Some(3)]);
    let scalar = session
        .sql("SELECT ZEROIFNULL(CAST(NULL AS BIGINT))")
        .await?
        .collect()
        .await?;
    let value = scalar[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(value.value(0), 0);
    Ok(())
}

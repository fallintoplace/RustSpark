use datafusion::arrow::array::Int64Array;
use rustspark::RustSparkSession;

#[tokio::test]
async fn try_negate_handles_vectors_nulls_and_overflow() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT TRY_NEGATE(a) AS result FROM \
             (VALUES (0, -7), (1, CAST('-9223372036854775808' AS BIGINT)), (2, NULL), (3, 0)) AS t(idx, a) ORDER BY idx",
        )
        .await?
        .collect()
        .await?;
    let mut actual = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        actual.extend(column.iter());
    }
    assert_eq!(actual, vec![Some(7), None, None, Some(0)]);
    let scalar = session
        .sql("SELECT TRY_NEGATE(-42)")
        .await?
        .collect()
        .await?;
    let values = scalar[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.value(0), 42);
    Ok(())
}

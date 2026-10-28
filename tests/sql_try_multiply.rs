use datafusion::arrow::array::Int64Array;
use rustspark::RustSparkSession;

#[tokio::test]
async fn try_multiply_handles_vectors_nulls_and_overflow() -> Result<(), Box<dyn std::error::Error>>
{
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT TRY_MULTIPLY(a, b) AS result FROM \
             (VALUES (0, 3, 4), (1, 9223372036854775807, 2), (2, NULL, 2), (3, -4, 3)) AS t(idx, a, b) ORDER BY idx",
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
    assert_eq!(actual, vec![Some(12), None, None, Some(-12)]);
    let scalar = session
        .sql("SELECT TRY_MULTIPLY(6, 7)")
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

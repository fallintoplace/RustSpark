use datafusion::arrow::array::Int64Array;
use rustspark::RustSparkSession;

#[tokio::test]
async fn shiftleft_supports_columns_nulls_and_literals() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT SHIFTLEFT(a, b) AS result FROM \
             (VALUES (0, 1, 3), (1, 1, 64), (2, NULL, 2), (3, -1, 1)) AS t(idx, a, b) ORDER BY idx",
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
    assert_eq!(actual, vec![Some(8), Some(1), None, Some(-2)]);
    let scalar = session
        .sql("SELECT SHIFTLEFT(3, 2)")
        .await?
        .collect()
        .await?;
    let values = scalar[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.value(0), 12);
    Ok(())
}

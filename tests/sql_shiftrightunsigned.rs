use datafusion::arrow::array::Int64Array;
use rustspark::RustSparkSession;

#[tokio::test]
async fn shiftrightunsigned_supports_rows_and_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT SHIFTRIGHTUNSIGNED(a, b) AS result FROM \
             (VALUES (0, -1, 1), (1, 8, 2), (2, 8, 64), (3, NULL, 0)) AS t(idx, a, b) ORDER BY idx",
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
    assert_eq!(
        actual,
        vec![Some(9223372036854775807), Some(2), Some(8), None]
    );
    let scalar = session
        .sql("SELECT SHIFTRIGHTUNSIGNED(16, 2)")
        .await?
        .collect()
        .await?;
    let values = scalar[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.value(0), 4);
    Ok(())
}

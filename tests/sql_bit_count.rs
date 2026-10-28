use datafusion::arrow::array::Int32Array;
use rustspark::RustSparkSession;

#[tokio::test]
async fn bit_count_supports_columns_nulls_and_literals() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT BIT_COUNT(a) AS result FROM \
             (VALUES (0, 5), (1, -1), (2, NULL), (3, 0)) AS t(idx, a) ORDER BY idx",
        )
        .await?
        .collect()
        .await?;
    let mut actual = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        actual.extend(column.iter());
    }
    assert_eq!(actual, vec![Some(2), Some(64), None, Some(0)]);
    let scalar = session
        .sql("SELECT BIT_COUNT(255)")
        .await?
        .collect()
        .await?;
    let values = scalar[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(values.value(0), 8);
    assert_eq!(
        scalar[0].schema().field(0).data_type(),
        &datafusion::arrow::datatypes::DataType::Int32,
    );
    Ok(())
}

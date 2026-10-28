use datafusion::arrow::array::Int8Array;
use rustspark::RustSparkSession;

#[tokio::test]
async fn bit_get_supports_rows_and_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT BIT_GET(a, b) AS result FROM \
             (VALUES (0, 5, 0), (1, 5, 1), (2, -1, 63), (3, NULL, 0)) AS t(idx, a, b) ORDER BY idx",
        )
        .await?
        .collect()
        .await?;
    let mut actual = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        actual.extend(column.iter());
    }
    assert_eq!(actual, vec![Some(1), Some(0), Some(1), None]);
    let scalar = session.sql("SELECT BIT_GET(8, 3)").await?.collect().await?;
    let values = scalar[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap();
    assert_eq!(values.value(0), 1);
    assert_eq!(
        scalar[0].schema().field(0).data_type(),
        &datafusion::arrow::datatypes::DataType::Int8,
    );
    if let Ok(invalid) = session.sql("SELECT BIT_GET(1, 64)").await {
        assert!(invalid.collect().await.is_err());
    }
    Ok(())
}

use datafusion::arrow::array::BooleanArray;
use rustspark::RustSparkSession;

#[tokio::test]
async fn equal_null_supports_rows_and_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT EQUAL_NULL(a, b) AS result FROM \
             (VALUES (0, 1, 1), (1, 1, 2), (2, NULL, NULL), (3, NULL, 1)) AS t(idx, a, b) ORDER BY idx",
        )
        .await?
        .collect()
        .await?;
    let mut actual = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        actual.extend(column.iter());
    }
    assert_eq!(actual, vec![Some(true), Some(false), Some(true), Some(false)]);
    let scalar = session.sql("SELECT EQUAL_NULL(2, 2)").await?.collect().await?;
    let values = scalar[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(values.value(0), true);
    Ok(())
}

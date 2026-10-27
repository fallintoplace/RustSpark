use datafusion::arrow::array::Float64Array;
use rustspark::RustSparkSession;

#[tokio::test]
async fn hypot_handles_rows_and_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT HYPOT(a, b) AS result FROM \
             (VALUES (0, CAST(3 AS DOUBLE), CAST(4 AS DOUBLE)), (1, CAST(-5 AS DOUBLE), CAST(12 AS DOUBLE)), (2, CAST(NULL AS DOUBLE), CAST(1 AS DOUBLE))) AS t(idx, a, b) ORDER BY idx",
        )
        .await?
        .collect()
        .await?;
    let mut actual = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        actual.extend(column.iter());
    }
    assert_eq!(actual, vec![Some(5.0), Some(13.0), None]);
    let scalar = session.sql("SELECT HYPOT(CAST(8 AS DOUBLE), CAST(15 AS DOUBLE))").await?.collect().await?;
    let values = scalar[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(values.value(0), 17.0);
    Ok(())
}

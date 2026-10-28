use datafusion::arrow::array::Float64Array;
use rustspark::RustSparkSession;

#[tokio::test]
async fn csc_handles_rows_and_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT CSC(a) AS result FROM \
             (VALUES (0, CAST(0 AS DOUBLE)), (1, CAST(NULL AS DOUBLE))) AS t(idx, a) ORDER BY idx",
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
    assert_eq!(actual, vec![Some(f64::INFINITY), None]);
    let scalar = session
        .sql("SELECT CSC(CAST(0 AS DOUBLE))")
        .await?
        .collect()
        .await?;
    let values = scalar[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(values.value(0), f64::INFINITY);
    Ok(())
}

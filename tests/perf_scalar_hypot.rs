use datafusion::arrow::array::Float64Array;
use rustspark::RustSparkSession;

#[tokio::test]
async fn hypot_with_scalar_rhs_preserves_rows() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session.sql(
        "SELECT HYPOT(n, CAST(4 AS DOUBLE)) FROM (VALUES (0, CAST(3 AS DOUBLE)), (1, CAST(-3 AS DOUBLE)), (2, CAST(NULL AS DOUBLE))) AS t(idx, n) ORDER BY idx",
    ).await?.collect().await?;
    let mut values = Vec::new();
    for batch in &batches {
        values.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter(),
        );
    }
    assert_eq!(values, vec![Some(5.0), Some(5.0), None]);
    Ok(())
}

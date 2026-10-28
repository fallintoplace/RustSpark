use datafusion::arrow::array::BooleanArray;
use rustspark::RustSparkSession;

#[tokio::test]
async fn startswith_scalar_rhs_preserves_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session.sql("SELECT STARTSWITH(s, 'sp') FROM (VALUES (0, 'spark'), (1, 'spin'), (2, 'ark'), (3, CAST(NULL AS VARCHAR))) AS t(idx, s) ORDER BY idx").await?.collect().await?;
    let mut values = Vec::new();
    for batch in &batches {
        values.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter(),
        );
    }
    assert_eq!(values, vec![Some(true), Some(true), Some(false), None]);
    Ok(())
}

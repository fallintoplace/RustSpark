use datafusion::arrow::array::Int64Array;
use rustspark::RustSparkSession;

#[tokio::test]
async fn width_bucket_handles_bounds_reverse_and_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT WIDTH_BUCKET(v, lo, hi, bins) FROM \
             (VALUES (0, 0.0, 0.0, 10.0, 5), \
                     (1, 2.5, 0.0, 10.0, 5), \
                     (2, 10.0, 0.0, 10.0, 5), \
                     (3, -1.0, 0.0, 10.0, 5), \
                     (4, NULL, 0.0, 10.0, 5), \
                     (5, 7.5, 10.0, 0.0, 5)) AS t(idx, v, lo, hi, bins) ORDER BY idx",
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
    assert_eq!(actual, vec![Some(1), Some(2), Some(6), Some(0), None, Some(2)]);
    if let Ok(invalid) = session.sql("SELECT WIDTH_BUCKET(1.0, 0.0, 0.0, 5)").await {
        assert!(invalid.collect().await.is_err());
    }
    Ok(())
}

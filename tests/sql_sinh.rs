use datafusion::arrow::array::Float64Array;
use rustspark::RustSparkSession;

#[tokio::test]
async fn spark_sinh_handles_scalars_arrays_nulls_and_bounds() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session.sql("SELECT SINH(v) FROM (VALUES (0, CAST(0.0 AS DOUBLE)), (1, CAST(1.0 AS DOUBLE)), (2, CAST(NULL AS DOUBLE))) AS t(idx, v) ORDER BY idx").await?.collect().await?;
    let actual: Vec<Option<f64>> = batches.iter()
        .flat_map(|batch| batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap().iter().collect::<Vec<_>>())
        .collect();
    let expected = [Some(0.0_f64), Some(1.1752011936438014), None];
    assert_eq!(actual.len(), expected.len());
    for (observed, expected) in actual.into_iter().zip(expected) {
        match (observed, expected) {
            (Some(got), Some(want)) if want.is_nan() => assert!(got.is_nan()),
            (Some(got), Some(want)) if want.is_infinite() => assert_eq!(got, want),
            (Some(got), Some(want)) => {
                assert!((got - want).abs() <= 1e-12 * want.abs().max(1.0), "expected {want}, got {got}");
            }
            (None, None) => {}
            (got, want) => panic!("expected {want:?}, got {got:?}"),
        }
    }

    let scalar = session.sql("SELECT SINH(CAST(0.0 AS DOUBLE))").await?.collect().await?;
    let values = scalar[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((values.value(0) - 0.0_f64).abs() <= 1e-12);
    Ok(())
}

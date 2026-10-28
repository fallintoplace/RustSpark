use datafusion::arrow::array::Int64Array;
use rustspark::RustSparkSession;

#[tokio::test]
async fn factorial_matches_spark_range_and_null_rules() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT FACTORIAL(n) FROM              (VALUES (0, CAST(0 AS BIGINT)), (1, CAST(5 AS BIGINT)),              (2, CAST(20 AS BIGINT)), (3, CAST(21 AS BIGINT)),              (4, CAST(-1 AS BIGINT)), (5, CAST(NULL AS BIGINT)))              AS t(idx, n) ORDER BY idx",
        )
        .await?
        .collect()
        .await?;
    let actual: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().iter()
        })
        .collect();
    assert_eq!(actual, vec![
        Some(1), Some(120), Some(2_432_902_008_176_640_000), None, None, None
    ]);

    let scalar = session.sql("SELECT FACTORIAL(CAST(6 AS BIGINT))").await?.collect().await?;
    let value = scalar[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(value.value(0), 720);
    Ok(())
}

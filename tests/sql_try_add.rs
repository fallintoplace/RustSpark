use datafusion::arrow::array::Int64Array;
use rustspark::RustSparkSession;

#[tokio::test]
async fn try_add_handles_rows_overflow_and_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let rows = session.sql(
        "SELECT TRY_ADD(a, b) FROM (VALUES          (0, CAST(1 AS BIGINT), CAST(2 AS BIGINT)),          (1, CAST(9223372036854775807 AS BIGINT), CAST(1 AS BIGINT)),          (2, CAST(NULL AS BIGINT), CAST(1 AS BIGINT)),          (3, CAST(-5 AS BIGINT), CAST(2 AS BIGINT))) AS t(idx, a, b) ORDER BY idx"
    ).await?.collect().await?;
    let actual: Vec<Option<i64>> = rows.iter().flat_map(|batch| {
        batch.column(0).as_any().downcast_ref::<Int64Array>()
            .expect("Int64 function output").iter()
    }).collect();
    assert_eq!(actual, vec![Some(3), None, None, Some(-3)]);
    let scalar = session.sql("SELECT try_add(20, 22)").await?.collect().await?;
    let values = scalar[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.value(0), 42);
    Ok(())
}

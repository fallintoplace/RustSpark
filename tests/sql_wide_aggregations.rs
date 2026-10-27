use datafusion::arrow::array::Int64Array;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn wide_aggregate_projection_keeps_all_columns_and_values() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "numbers",
            "SELECT CAST(1 AS BIGINT) AS value
             UNION ALL SELECT CAST(2 AS BIGINT) AS value",
        )
        .await?;

    let projection = (0..90)
        .map(|index| format!("SUM(value + {index}) AS s{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {projection} FROM numbers");
    let batches = session.sql(&sql).await?.collect().await?;

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 90);
    for index in 0..90 {
        assert_eq!(batch.schema().field(index).name(), &format!("s{index}"));
        let values = batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("sum of big integers");
        assert_eq!(values.value(0), 3 + 2 * index as i64);
    }
    Ok(())
}

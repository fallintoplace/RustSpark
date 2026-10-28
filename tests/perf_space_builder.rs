use datafusion::arrow::array::StringArray;
use rustspark::RustSparkSession;

#[tokio::test]
async fn space_builder_keeps_short_long_negative_and_null_rows() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session.sql(
        "SELECT SPACE(n) FROM (VALUES (0, CAST(0 AS BIGINT)), (1, CAST(3 AS BIGINT)), (2, CAST(70 AS BIGINT)), (3, CAST(-2 AS BIGINT)), (4, CAST(NULL AS BIGINT))) AS t(idx, n) ORDER BY idx",
    ).await?.collect().await?;
    let mut values = Vec::new();
    for batch in &batches {
        values.extend(batch.column(0).as_any().downcast_ref::<StringArray>().unwrap().iter().map(|value| value.map(str::to_owned)));
    }
    assert_eq!(values, vec![Some(String::new()), Some(" ".repeat(3)), Some(" ".repeat(70)), Some(String::new()), None]);
    Ok(())
}

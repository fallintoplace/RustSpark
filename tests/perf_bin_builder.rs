use datafusion::arrow::array::StringArray;
use rustspark::RustSparkSession;

#[tokio::test]
async fn bin_builder_handles_negative_zero_and_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session.sql(
        "SELECT BIN(n) FROM (VALUES (0, CAST(10 AS BIGINT)), (1, CAST(-7 AS BIGINT)), (2, CAST(0 AS BIGINT)), (3, CAST(NULL AS BIGINT))) AS t(idx, n) ORDER BY idx",
    ).await?.collect().await?;
    let mut values = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        values.extend(column.iter().map(|value| value.map(str::to_owned)));
    }
    assert_eq!(
        values,
        vec![
            Some("1010".to_owned()),
            Some(format!("{:b}", -7_i64 as u64)),
            Some("0".to_owned()),
            None
        ]
    );
    Ok(())
}

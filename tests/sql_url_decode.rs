use datafusion::arrow::array::StringArray;
use rustspark::RustSparkSession;

#[tokio::test]
async fn url_decode_handles_nulls_and_utf8_rows() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT URL_DECODE(a) AS result FROM \
             (VALUES (0, 'hello%20world%2F%2B'), (1, '%C3%A9'), (2, NULL)) AS t(idx, a) ORDER BY idx",
        )
        .await?
        .collect()
        .await?;
    let mut actual = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        actual.extend(column.iter().map(|value| value.map(str::to_owned)));
    }
    assert_eq!(actual, vec![
            Some("hello world/+".to_owned()),
            Some("é".to_owned()),
            None,
        ]);
    let scalar = session.sql("SELECT URL_DECODE('a%20b')").await?.collect().await?;
    let values = scalar[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), "a b");
    if let Ok(invalid) = session.sql("SELECT URL_DECODE('%ZZ')").await {
        assert!(invalid.collect().await.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn url_decode_distinguishes_plus_from_percent_encoded_plus() -> Result<(), Box<dyn std::error::Error>> {
    let batches = RustSparkSession::new()
        .sql("SELECT URL_DECODE('a+b%2Bc')").await?.collect().await?;
    let values = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(values.value(0), "a b+c");
    Ok(())
}

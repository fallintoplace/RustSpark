use datafusion::arrow::array::StringArray;
use rustspark::RustSparkSession;

#[tokio::test]
async fn url_encode_handles_nulls_and_utf8_rows() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT URL_ENCODE(a) AS result FROM \
             (VALUES (0, 'hello world/+'), (1, 'é'), (2, NULL)) AS t(idx, a) ORDER BY idx",
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
            Some("hello%20world%2F%2B".to_owned()),
            Some("%C3%A9".to_owned()),
            None,
        ]);
    let scalar = session.sql("SELECT URL_ENCODE('a b')").await?.collect().await?;
    let values = scalar[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), "a%20b");
    Ok(())
}

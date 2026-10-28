use datafusion::arrow::array::StringArray;
use rustspark::RustSparkSession;

#[tokio::test]
async fn soundex_builder_handles_ascii_codes_and_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session.sql(
        "SELECT SOUNDEX(name) FROM (VALUES (0, 'Robert'), (1, 'Ashcraft'), (2, '123'), (3, CAST(NULL AS VARCHAR)), (4, 'Pfister')) AS t(idx, name) ORDER BY idx",
    ).await?.collect().await?;
    let mut values = Vec::new();
    for batch in &batches {
        values.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|value| value.map(str::to_owned)),
        );
    }
    assert_eq!(
        values,
        vec![
            Some("R163".to_owned()),
            Some("A261".to_owned()),
            Some(String::new()),
            None,
            Some("P236".to_owned())
        ]
    );
    Ok(())
}

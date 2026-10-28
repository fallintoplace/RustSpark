use datafusion::arrow::array::StringArray;
use rustspark::RustSparkSession;

#[tokio::test]
async fn soundex_handles_rows_and_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT SOUNDEX(value) FROM \
             (VALUES (0, 'Robert'), (1, 'Rupert'), (2, 'Ashcraft'), (3, NULL), (4, '')) AS t(idx, value) ORDER BY idx",
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
    assert_eq!(
        actual,
        vec![
            Some("R163".to_owned()),
            Some("R163".to_owned()),
            Some("A261".to_owned()),
            None,
            Some(String::new()),
        ]
    );
    let scalar = session
        .sql("SELECT SOUNDEX('Pfister')")
        .await?
        .collect()
        .await?;
    let values = scalar[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), "P236");
    Ok(())
}

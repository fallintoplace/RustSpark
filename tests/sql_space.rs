use datafusion::arrow::array::StringArray;
use rustspark::RustSparkSession;

#[tokio::test]
async fn space_supports_rows_and_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT SPACE(n) AS result FROM \
             (VALUES (0, 3), (1, 0), (2, -2), (3, NULL)) AS t(idx, n) \
             ORDER BY idx",
        )
        .await?
        .collect()
        .await?;
    let mut result = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        result.extend(column.iter().map(|value| value.map(str::to_owned)));
    }
    assert_eq!(
        result,
        vec![
            Some("   ".to_owned()),
            Some(String::new()),
            Some(String::new()),
            None,
        ]
    );
    Ok(())
}

#[tokio::test]
async fn space_rejects_unbounded_padding() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    match session.sql("SELECT SPACE(1048577)").await {
        Ok(dataframe) => assert!(dataframe.collect().await.is_err()),
        Err(_) => {}
    }
    Ok(())
}

#[test]
fn space_is_available_in_cli_sql() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT SPACE(3) AS result",
            "--stdout-format",
            "json-array",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("   "), "{stdout}");
    Ok(())
}

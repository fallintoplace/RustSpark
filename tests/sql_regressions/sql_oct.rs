use datafusion::arrow::array::StringArray;
use rustspark::RustSparkSession;

#[tokio::test]
async fn oct_supports_rows_and_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT OCT(n) AS result FROM \
             (VALUES (0, 9), (1, 0), (2, -1), (3, NULL)) AS t(idx, n) \
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
            Some("11".to_owned()),
            Some("0".to_owned()),
            Some(format!("{:o}", u64::MAX)),
            None,
        ]
    );
    Ok(())
}

#[test]
fn oct_is_available_in_cli_sql() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT OCT(9) AS result",
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
    assert!(stdout.contains("11"), "{stdout}");
    Ok(())
}

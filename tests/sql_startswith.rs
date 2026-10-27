use datafusion::arrow::array::BooleanArray;
use rustspark::RustSparkSession;

#[tokio::test]
async fn startswith_supports_rows_and_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT STARTSWITH(s, p) AS result FROM \
             (VALUES (0, 'spark', 'sp'), (1, 'spark', 'ark'), \
                     (2, 'éclair', 'é'), (3, NULL, 'a'), (4, 'a', NULL)) AS t(idx, s, p) \
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
            .downcast_ref::<BooleanArray>()
            .unwrap();
        result.extend(column.iter());
    }
    assert_eq!(
        result,
        vec![Some(true), Some(false), Some(true), None, None]
    );
    Ok(())
}

#[test]
fn startswith_is_available_in_cli_sql() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT STARTSWITH('spark', 'sp') AS result",
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
    assert!(stdout.contains("true"), "{stdout}");
    Ok(())
}

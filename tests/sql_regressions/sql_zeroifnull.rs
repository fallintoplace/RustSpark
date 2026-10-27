use datafusion::arrow::array::Int64Array;
use rustspark::RustSparkSession;

#[tokio::test]
async fn zeroifnull_works_with_rows_and_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT ZEROIFNULL(n) AS result FROM \
             (VALUES (0, NULL), (1, 5), (2, 0), (3, -2)) AS t(idx, n) \
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
            .downcast_ref::<Int64Array>()
            .unwrap();
        result.extend(column.iter());
    }
    assert_eq!(result, vec![Some(0), Some(5), Some(0), Some(-2)]);
    Ok(())
}

#[test]
fn zeroifnull_is_available_in_cli_sql() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT ZEROIFNULL(7) AS result",
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
    assert!(stdout.contains("7"), "{stdout}");
    Ok(())
}

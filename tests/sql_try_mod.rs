use datafusion::arrow::array::Int64Array;
use rustspark::RustSparkSession;

#[tokio::test]
async fn try_mod_works_with_rows_and_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT TRY_MOD(n, d) AS result FROM \
             (VALUES (0, 9, 2), (1, 4, 0), (2, NULL, 3), (3, -7, 3)) AS t(idx, n, d) \
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
        result.extend(column.iter().map(|value| value));
    }
    assert_eq!(result, vec![Some(1), None, None, Some(-1)]);
    Ok(())
}

#[test]
fn try_mod_is_available_in_cli_sql() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT TRY_MOD(7, 2) AS result",
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
    assert!(stdout.contains("1"), "{stdout}");
    Ok(())
}

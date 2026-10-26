use std::process::Command;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn sales_csv() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")
}

#[test]
fn writes_csv_as_a_single_file() -> TestResult {
    let dir = tempfile::tempdir()?;
    let output_path = dir.path().join("results");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--csv", sales_csv(), "--sql", "SELECT city FROM data"])
        .arg("--output-csv")
        .arg(&output_path)
        .arg("--output-single-file")
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output_path.is_file());
    let body = std::fs::read_to_string(&output_path)?;
    assert!(body.contains("Berlin"));
    assert!(body.contains("Munich"));
    Ok(())
}

#[test]
fn writes_json_as_a_single_file() -> TestResult {
    let dir = tempfile::tempdir()?;
    let output_path = dir.path().join("result");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 7 AS value"])
        .arg("--output-json")
        .arg(&output_path)
        .arg("--output-single-file")
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output_path.is_file());
    assert!(std::fs::read_to_string(&output_path)?.contains("7"));
    Ok(())
}

#[tokio::test]
async fn writes_parquet_as_a_single_file() -> TestResult {
    let dir = tempfile::tempdir()?;
    let output_path = dir.path().join("result.parquet");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--csv", sales_csv(), "--sql", "SELECT amount FROM data"])
        .arg("--output-parquet")
        .arg(&output_path)
        .arg("--output-single-file")
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output_path.is_file());
    let session = rustspark::RustSparkSession::new();
    assert_eq!(session.read_parquet(&output_path).await?.count().await?, 3);
    Ok(())
}

#[test]
fn rejects_single_file_mode_without_export() -> TestResult {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 1", "--output-single-file"])
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires an output format"));
    Ok(())
}

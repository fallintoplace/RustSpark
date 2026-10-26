use std::{fs::File, process::Command};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn sales_csv() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")
}

#[tokio::test]
async fn writes_arrow_ipc_file_without_collecting_all_results() -> TestResult {
    let dir = tempfile::tempdir()?;
    let output_file = dir.path().join("sales.arrow");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--batch-size",
            "1",
            "--sql",
            "SELECT city, amount FROM data ORDER BY amount",
        ])
        .arg("--output-arrow")
        .arg(&output_file)
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Validate with Arrow's native reader rather than a text search.
    let file = File::open(&output_file)?;
    let mut ipc = datafusion::arrow::ipc::reader::FileReader::try_new(file, None)?;
    let schema = ipc.schema();
    assert_eq!(schema.field(0).name(), "city");
    assert_eq!(schema.field(1).name(), "amount");
    let mut rows = 0;
    for batch in &mut ipc {
        rows += batch?.num_rows();
    }
    assert_eq!(rows, 3);
    Ok(())
}

#[test]
fn empty_query_still_writes_valid_ipc_schema() -> TestResult {
    let dir = tempfile::tempdir()?;
    let output_file = dir.path().join("empty.arrow");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT CAST(1 AS INT) AS id WHERE FALSE"])
        .arg("--output-arrow")
        .arg(&output_file)
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let file = File::open(&output_file)?;
    let mut ipc = datafusion::arrow::ipc::reader::FileReader::try_new(file, None)?;
    assert_eq!(ipc.schema().field(0).name(), "id");
    assert!(ipc.next().is_none());
    Ok(())
}

#[test]
fn disallows_conflicting_output_modes() -> TestResult {
    let dir = tempfile::tempdir()?;
    let arrow = dir.path().join("output.arrow");
    for incompatible in ["--max-rows", "--output-csv"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--sql", "SELECT 1"])
            .arg("--output-arrow")
            .arg(&arrow)
            .arg(incompatible)
            .arg("1")
            .output()?;
        assert!(!output.status.success());
    }
    Ok(())
}

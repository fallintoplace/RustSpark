use std::{fs, path::Path, process::Command};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn assert_row_count(path: &Path, expected: &str) -> TestResult {
    let result = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--input")
        .arg(path)
        .args([
            "--sql",
            "SELECT COUNT(*) AS total FROM data",
            "--stdout-format",
            "csv",
        ])
        .output()?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(String::from_utf8(result.stdout)?, expected);
    Ok(())
}

#[test]
fn auto_input_keeps_original_case_for_reader_extension() -> TestResult {
    let dir = tempfile::tempdir()?;
    for (fixture, name) in [
        ("tests/fixtures/sales.csv", "sales.CSV"),
        ("tests/fixtures/sales.json", "sales.JSON"),
        ("tests/fixtures/sales.json", "sales.NdJsOn"),
    ] {
        let path = dir.path().join(name);
        fs::copy(fixture, &path)?;
        assert_row_count(&path, "total\n3\n")?;
    }

    let parquet = dir.path().join("saved.PARQUET");
    let export = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 42 AS value", "--output-parquet"])
        .arg(&parquet)
        .arg("--output-single-file")
        .output()?;
    assert!(
        export.status.success(),
        "{}",
        String::from_utf8_lossy(&export.stderr)
    );
    assert_row_count(&parquet, "total\n1\n")?;
    Ok(())
}

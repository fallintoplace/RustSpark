use std::process::{Command, Output};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(args: &[&str]) -> std::io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(args)
        .output()
}

#[test]
fn auto_input_loads_csv_and_json() -> TestResult {
    for path in ["tests/fixtures/sales.csv", "tests/fixtures/sales.json"] {
        let output = run(&[
            "--input",
            path,
            "--sql",
            "SELECT COUNT(*) AS total FROM data",
            "--stdout-format",
            "csv",
        ])?;
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        assert_eq!(String::from_utf8(output.stdout)?, "total\n3\n");
    }
    Ok(())
}

#[test]
fn auto_input_loads_generated_parquet() -> TestResult {
    let directory = tempfile::tempdir()?;
    let parquet = directory.path().join("result.parquet");
    let written = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 42 AS answer", "--output-parquet"])
        .arg(&parquet)
        .arg("--output-single-file")
        .output()?;
    assert!(written.status.success(), "{}", String::from_utf8_lossy(&written.stderr));

    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--input")
        .arg(&parquet)
        .args(["--sql", "SELECT COUNT(*) AS total FROM data", "--stdout-format", "csv"])
        .output()?;
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(String::from_utf8(output.stdout)?, "total\n1\n");
    Ok(())
}

#[test]
fn auto_input_rejects_ambiguous_or_unsupported_paths() -> TestResult {
    let ambiguous = run(&[
        "--input",
        "tests/fixtures/sales.csv",
        "--csv",
        "tests/fixtures/sales.csv",
        "--sql",
        "SELECT 1",
    ])?;
    assert!(!ambiguous.status.success());

    let unsupported = run(&["--input", "archive.zip", "--sql", "SELECT 1"])?;
    assert!(!unsupported.status.success());
    assert!(String::from_utf8_lossy(&unsupported.stderr).contains("Unsupported --input extension"));

    let directory = run(&["--input", "tests/fixtures", "--sql", "SELECT 1"])?;
    assert!(!directory.status.success());
    assert!(String::from_utf8_lossy(&directory.stderr).contains("--input needs a file"));
    Ok(())
}

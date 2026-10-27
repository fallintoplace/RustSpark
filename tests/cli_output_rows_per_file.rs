use std::{fs, process::Command};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn splits_large_partitioned_exports_into_multiple_parquet_parts() -> TestResult {
    let dir = tempfile::tempdir()?;
    let export = dir.path().join("export");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT n FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9)) AS items(n)",
            "--target-partitions",
            "1",
            "--batch-size",
            "1",
            "--output-rows-per-file",
            "2",
            "--output-parquet",
        ])
        .arg(&export)
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let parquet_files = fs::read_dir(&export)?
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "parquet"))
        .count();
    assert!(parquet_files >= 2, "expected multiple Parquet parts, found {parquet_files}");

    let read = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--parquet")
        .arg(&export)
        .args([
            "--sql",
            "SELECT COUNT(*) AS rows FROM data",
            "--stdout-format",
            "json",
        ])
        .output()?;
    assert!(
        read.status.success(),
        "{}",
        String::from_utf8_lossy(&read.stderr)
    );
    assert_eq!(String::from_utf8(read.stdout)?.trim(), r#"{"rows":9}"#);
    Ok(())
}

#[test]
fn validates_export_file_size_options() -> TestResult {
    for rows in ["0", "-1", "not-a-number"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--sql", "SELECT 1", "--output-rows-per-file", rows])
            .output()?;
        assert!(!output.status.success(), "{rows}");
    }
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 1", "--output-rows-per-file", "2"])
        .output()?;
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("requires a file export")
    );
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT 1",
            "--output-single-file",
            "--output-rows-per-file",
            "2",
        ])
        .output()?;
    assert!(!output.status.success());
    Ok(())
}

use std::{fs::File, io::Write, process::Command};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn reads_large_csv_with_custom_scan_split_threshold() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("numbers.csv");
    let mut csv = File::create(&path)?;
    writeln!(csv, "n")?;
    for n in 0..20_000 {
        writeln!(csv, "{n}")?;
    }
    drop(csv);

    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(&path)
        .args([
            "--csv-schema",
            "n:int64",
            "--target-partitions",
            "4",
            "--scan-split-min-kb",
            "8",
            "--sql",
            "SELECT COUNT(*) AS rows FROM data",
            "--stdout-format",
            "json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8(output.stdout)?.trim(), r#"{"rows":20000}"#);
    Ok(())
}

#[test]
fn rejects_invalid_split_thresholds() -> TestResult {
    for threshold in ["0", "-1", "not-a-number", &usize::MAX.to_string()] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--sql", "SELECT 1", "--scan-split-min-kb", threshold])
            .output()?;
        assert!(!output.status.success(), "{threshold}");
    }
    Ok(())
}

use std::{fs::File, io::Write, process::Command};

#[test]
fn streams_large_csv_results_without_missing_rows() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("input.csv");
    let mut file = File::create(&path)?;
    writeln!(file, "n")?;
    for n in 0..8_192 {
        writeln!(file, "{n}")?;
    }
    drop(file);
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(&path)
        .args([
            "--csv-schema",
            "n:int64",
            "--sql",
            "SELECT n FROM data",
            "--stdout-format",
            "csv",
            "--batch-size",
            "64",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let body = String::from_utf8(output.stdout)?;
    let mut lines = body.lines();
    assert_eq!(lines.next(), Some("n"));
    assert_eq!(lines.clone().count(), 8_192);
    assert_eq!(lines.next(), Some("0"));
    assert_eq!(body.lines().last(), Some("8191"));
    Ok(())
}

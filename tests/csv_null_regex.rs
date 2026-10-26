use std::process::Command;

#[test]
fn custom_null_regex_matches_missing_values() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let csv = dir.path().join("nullable.csv");
    std::fs::write(&csv, "city,amount\nBerlin,10\nMunich,NA\nHamburg,20\n")?;

    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            csv.to_str().unwrap(),
            "--csv-schema-rows",
            "1",
            "--csv-null-regex",
            "^NA$",
            "--sql",
            "SELECT COUNT(*) FROM data WHERE amount IS NULL",
        ])
        .output()?;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains('1'));
    Ok(())
}

#[test]
fn rejects_null_regex_without_csv_input() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 1", "--csv-null-regex", "^NA$"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("requires a CSV input")
    );
}

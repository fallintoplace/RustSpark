use std::{fs, process::Command};

#[test]
fn fills_missing_csv_trailing_columns_with_null() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("short_rows.csv");
    fs::write(&file, "city,amount\nBerlin,20\nMunich\n").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(&file)
        .args([
            "--csv-allow-short-rows",
            "--sql",
            "SELECT COUNT(*) FROM data WHERE amount IS NULL",
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(String::from_utf8_lossy(&output.stdout).contains("1"));
}

#[test]
fn rejects_short_row_option_without_csv() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--json",
            "tests/fixtures/sales.json",
            "--csv-allow-short-rows",
            "--sql",
            "SELECT 1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires a CSV"));
}

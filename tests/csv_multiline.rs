use std::{fs, process::Command};

#[test]
fn reads_newline_inside_quoted_csv_field() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("multiline.csv");
    fs::write(&file, "note,amount\n\"hello\nworld\",5\n\"plain\",7\n").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(&file)
        .args(["--csv-multiline", "--sql", "SELECT SUM(amount) FROM data"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("12"));
}

#[test]
fn rejects_csv_multiline_without_csv() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--json",
            "tests/fixtures/sales.json",
            "--csv-multiline",
            "--sql",
            "SELECT 1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires a CSV"));
}

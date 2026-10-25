use std::{fs, process::Command};

#[test]
fn reads_backslash_escaped_quotes_in_csv() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("escaped.csv");
    fs::write(&file, "name,amount\n\"say \\\"hi\\\"\",5\n").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(&file)
        .args([
            "--csv-escape",
            "\\",
            "--sql",
            "SELECT SUM(amount) FROM data WHERE name = 'say \"hi\"'",
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(String::from_utf8_lossy(&output.stdout).contains("5"));
}

#[test]
fn rejects_multibyte_csv_escape_character() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            "tests/fixtures/sales.csv",
            "--csv-escape",
            "🔥",
            "--sql",
            "SELECT 1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

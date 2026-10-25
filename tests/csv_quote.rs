use std::{fs, process::Command};

#[test]
fn reads_csv_with_custom_quote_character() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("quoted.csv");
    fs::write(&file, "name,amount\n'New,York',5\n'Old,Town',7\n").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(&file)
        .args([
            "--csv-quote",
            "'",
            "--sql",
            "SELECT SUM(amount) FROM data WHERE name = 'New,York'",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("5"));
}

#[test]
fn rejects_invalid_csv_quotes() {
    for quote in ["ab", "🔥", ","] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args([
                "--csv",
                "tests/fixtures/sales.csv",
                "--csv-quote",
                quote,
                "--sql",
                "SELECT 1",
            ])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{quote}");
    }
}

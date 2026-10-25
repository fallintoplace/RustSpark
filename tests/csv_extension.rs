use std::{fs, process::Command};

#[test]
fn reads_csv_with_nonstandard_file_extension() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("sales.data");
    fs::write(&file, "city,amount\nBerlin,20\nMunich,5\n").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(&file)
        .args([
            "--csv-extension",
            ".data",
            "--sql",
            "SELECT SUM(amount) FROM data",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("25"));
}

#[test]
fn rejects_malformed_csv_extensions() {
    for extension in ["data", ".", ".txt.gz"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args([
                "--csv",
                "tests/fixtures/sales.csv",
                "--csv-extension",
                extension,
                "--sql",
                "SELECT 1",
            ])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{extension}");
    }
}

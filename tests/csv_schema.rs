use std::process::Command;

#[test]
fn explicit_csv_schema_preserves_string_identifiers() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let csv = dir.path().join("coded.csv");
    std::fs::write(&csv, "code,amount\n001,10\n002,20\n")?;

    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            csv.to_str().unwrap(),
            "--csv-schema",
            "code:utf8,amount:int64",
            "--sql",
            "SELECT MIN(code) AS first_code, SUM(amount) AS total FROM data",
        ])
        .output()?;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("001"), "{stdout}");
    assert!(stdout.contains("30"), "{stdout}");
    Ok(())
}

#[test]
fn invalid_schema_declarations_are_rejected() {
    for schema in [
        "missingtype",
        "1bad:utf8",
        "code:unknown",
        "code:utf8,code:int64",
        "",
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--csv", "tests/fixtures/sales.csv", "--csv-schema", schema, "--sql", "SELECT 1"])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{schema}");
    }
}

#[test]
fn explicit_schema_requires_csv_input() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 1", "--csv-schema", "code:utf8"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires a CSV input"));
}

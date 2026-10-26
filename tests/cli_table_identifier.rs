use std::process::Command;

#[test]
fn rejects_invalid_unnamed_table_identifiers() {
    for name in ["2data", "data.table", "sales-data"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--table", name, "--sql", "SELECT 1"])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{name}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("SQL identifier"),
            "{name}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn accepts_valid_unnamed_table_identifiers() {
    let source = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv", source, "--table", "daily_sales",
            "--sql", "SELECT COUNT(*) FROM daily_sales",
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
}

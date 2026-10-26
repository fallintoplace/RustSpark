use std::process::Command;

#[test]
fn prints_query_schema_without_executing_query() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            "tests/fixtures/sales.csv",
            "--sql",
            "SELECT city, amount FROM data",
            "--describe",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("city\tUtf8\t"), "{stdout}");
    assert!(stdout.contains("amount\tInt64\t"), "{stdout}");
    assert!(!stdout.contains("Berlin"), "{stdout}");
}

#[test]
fn describe_conflicts_with_export() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 1", "--describe", "--output-csv", "export"])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

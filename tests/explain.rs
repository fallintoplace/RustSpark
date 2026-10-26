use std::process::Command;

#[test]
fn explains_sql_plans_instead_of_running_a_query() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            "tests/fixtures/sales.csv",
            "--sql",
            "SELECT city FROM data WHERE amount > 5",
            "--explain",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("TableScan"), "{stdout}");
    assert!(!stdout.contains("Berlin"), "{stdout}");
}

#[test]
fn explain_rejects_export_flags() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT 1",
            "--explain",
            "--output-parquet",
            "export",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

#[test]
fn explain_rejects_describe_mode() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 1", "--describe", "--explain"])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

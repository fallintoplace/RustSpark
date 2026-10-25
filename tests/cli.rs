use std::process::Command;

fn sales_csv() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")
}

#[test]
fn help_explains_options() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--help")
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("--csv"));
    assert!(stdout.contains("--sql"));
}

#[test]
fn runs_a_query_from_the_command_line() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            &sales_csv(),
            "--table",
            "sales",
            "--sql",
            "SELECT SUM(amount) AS total FROM sales",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8(output.stdout).unwrap().contains("35"));
}

#[test]
fn reports_missing_required_arguments() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(sales_csv())
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr).unwrap().contains("--sql"));
}

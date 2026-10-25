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
    assert!(stdout.contains("--parquet"));
    assert!(stdout.contains("--sql"));
}

#[test]
fn runs_a_query_from_the_command_line() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
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
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "35"),
        "{stdout}"
    );
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

#[test]
fn runs_query_with_default_table_name() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--sql",
            "SELECT COUNT(*) AS total FROM data",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "3"),
        "{stdout}"
    );
}

#[test]
fn reports_a_missing_csv_file() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            "tests/fixtures/this_file_does_not_exist.csv",
            "--sql",
            "SELECT * FROM data",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(!output.stderr.is_empty());
}

#[test]
fn requires_exactly_one_input_format() {
    let binary = env!("CARGO_BIN_EXE_rustspark");
    let no_input = Command::new(binary)
        .args(["--sql", "SELECT 1"])
        .output()
        .unwrap();
    assert!(!no_input.status.success());

    let both = Command::new(binary)
        .args([
            "--csv",
            sales_csv(),
            "--parquet",
            sales_csv(),
            "--sql",
            "SELECT 1",
        ])
        .output()
        .unwrap();
    assert!(!both.status.success());
}

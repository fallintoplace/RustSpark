use std::process::Command;

fn sales_csv() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")
}

fn sales_json() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.json")
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
    assert!(stdout.contains("--json"));
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

#[test]
fn queries_json_from_the_command_line() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--json",
            sales_json(),
            "--sql",
            "SELECT SUM(amount) AS total FROM data",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "35")
    );
}

#[test]
fn refuses_two_input_formats() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--json",
            sales_json(),
            "--sql",
            "SELECT 1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

#[test]
fn runs_query_from_sql_file() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--sql-file",
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/count_sales.sql"
            ),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "3")
    );
}

#[test]
fn rejects_two_query_sources() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--sql",
            "SELECT 1",
            "--sql-file",
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/count_sales.sql"
            ),
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

#[test]
fn reports_missing_sql_file() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--csv", sales_csv(), "--sql-file", "missing.sql"])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

#[test]
fn parses_semicolon_delimited_csv() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/sales_semicolon.csv"
            ),
            "--csv-delimiter",
            ";",
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
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "15")
    );
}

#[test]
fn rejects_invalid_csv_delimiters() {
    for delimiter in ["::", "🔥"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args([
                "--csv",
                sales_csv(),
                "--csv-delimiter",
                delimiter,
                "--sql",
                "SELECT 1",
            ])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{delimiter}");
    }
}

#[test]
fn rejects_csv_delimiter_for_parquet() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--parquet",
            sales_csv(),
            "--csv-delimiter",
            ";",
            "--sql",
            "SELECT 1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--csv-delimiter requires --csv"));
}

#[test]
fn joins_multiple_named_csv_tables() {
    let left = format!("left_sales={}", sales_csv());
    let right = format!("right_sales={}", sales_csv());
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv-table",
            left.as_str(),
            "--csv-table",
            right.as_str(),
            "--sql",
            "SELECT COUNT(*) FROM left_sales JOIN right_sales USING (city)",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "5")
    );
}

#[test]
fn rejects_invalid_named_csv_inputs() {
    for mapping in ["missing_equals", "=file.csv", "123=path.csv", "sales="] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--csv-table", mapping, "--sql", "SELECT 1"])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{mapping}");
    }
}

#[test]
fn rejects_named_and_unnamed_csv_together() {
    let mapping = format!("sales={}", sales_csv());
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--csv-table",
            mapping.as_str(),
            "--sql",
            "SELECT 1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

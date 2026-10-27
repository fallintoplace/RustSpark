use std::{fs, process::Command};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn assert_row(output: std::process::Output, row: &str) -> TestResult {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == row),
        "{stdout}"
    );
    Ok(())
}

#[test]
fn runs_sequential_sql_statements_with_shared_session() -> TestResult {
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("report.sql");
    fs::write(
        &file,
        "CREATE VIEW filtered AS SELECT amount FROM data WHERE amount >= 10;\n\
         SELECT SUM(amount) FROM filtered;",
    )?;
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--csv", "tests/fixtures/sales.csv", "--sql-file"])
        .arg(&file)
        .output()?;
    assert_row(output, "30")
}

#[test]
fn leaves_semicolons_in_sql_string_literals_intact() -> TestResult {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT 'earlier;query'; -- commented ; semicolon\nSELECT 'final;value' AS answer;",
        ])
        .output()?;
    assert_row(output, "final;value")
}

#[test]
fn only_the_last_statement_is_displayed() -> TestResult {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 1111111; SELECT 2222222"])
        .output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "2222222"),
        "{stdout}"
    );
    assert!(!stdout.contains("1111111"));
    Ok(())
}

#[test]
fn stops_on_failed_intermediate_statement() -> TestResult {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT * FROM table_that_does_not_exist; SELECT 42",
        ])
        .output()?;
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("42"));
    Ok(())
}

#[test]
fn rejects_comment_only_scripts() -> TestResult {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--sql=-- only a comment")
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("at least one statement"));
    Ok(())
}

#[test]
fn query_timeout_covers_the_entire_script() -> TestResult {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 1; SELECT 2", "--query-timeout", "30"])
        .output()?;
    assert_row(output, "2")
}

#[test]
fn binds_positional_parameters_in_earlier_script_statements() -> TestResult {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT $1 AS before; SELECT $1 + 1 AS answer",
            "--param",
            "int64:41",
        ])
        .output()?;
    assert_row(output, "42")
}

#[test]
fn binds_named_parameters_in_earlier_script_statements() -> TestResult {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT $value AS before; SELECT $value + 1 AS answer",
            "--named-param",
            "value=int64:41",
        ])
        .output()?;
    assert_row(output, "42")
}

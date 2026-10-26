use std::{
    io::Write,
    process::{Command, Stdio},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn shell(script: &str) -> TestResultOutput {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--interactive", "--csv", "tests/fixtures/sales.csv"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("starts shell");
    child
        .stdin
        .take()
        .expect("stdin piped")
        .write_all(script.as_bytes())
        .expect("writes SQL");
    child.wait_with_output().expect("shell exits")
}

type TestResultOutput = std::process::Output;

#[test]
fn shell_runs_multiple_sql_statements_in_one_session() -> TestResult {
    let output = shell(
        "CREATE VIEW large AS SELECT amount FROM data WHERE amount >= 10;\n\
         SELECT SUM(amount) AS total FROM large;\n\
         SELECT * FROM missing_table;\n\
         SELECT 7 AS answer;\n\\q\n",
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "30"),
        "{stdout}"
    );
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "7"),
        "{stdout}"
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("SQL error"));
    Ok(())
}

#[test]
fn shell_preserves_multiline_sql_and_semicolons_in_literals() -> TestResult {
    let output = shell("SELECT 'hello;world' AS word\n;\n\\q\n");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("hello;world"));
    Ok(())
}

#[test]
fn shell_reports_unfinished_sql_on_end_of_input() {
    let output = shell("SELECT 1");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("terminating semicolon"));
}

#[test]
fn interactive_mode_conflicts_with_one_shot_query_flags() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--interactive", "--sql", "SELECT 1"])
        .output()
        .expect("CLI runs");
    assert!(!output.status.success());
}

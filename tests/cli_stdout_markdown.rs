use std::process::{Command, Output};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(sql: &str, other_args: &[&str]) -> std::io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", sql, "--stdout-format", "markdown"])
        .args(other_args)
        .output()
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn markdown_streams_rows_across_batches() -> TestResult {
    let output = run(
        "SELECT id FROM (VALUES (3), (1), (2)) AS t(id) ORDER BY id",
        &["--batch-size", "1"],
    )?;
    assert_success(&output);
    assert_eq!(
        String::from_utf8(output.stdout)?,
        "| id |\\n| --- |\\n| 1 |\\n| 2 |\\n| 3 |\\n"
    );
    Ok(())
}

#[test]
fn markdown_escapes_pipes_and_prints_null_explicitly() -> TestResult {
    let output = run(
        "SELECT 'a|b' AS value, CAST(NULL AS VARCHAR) AS missing",
        &[],
    )?;
    assert_success(&output);
    assert_eq!(
        String::from_utf8(output.stdout)?,
        "| value | missing |\\n| --- | --- |\\n| a\\\\|b | NULL |\\n"
    );
    Ok(())
}

#[test]
fn markdown_empty_result_has_column_headers() -> TestResult {
    let output = run("SELECT 42 AS answer WHERE FALSE", &[])?;
    assert_success(&output);
    assert_eq!(
        String::from_utf8(output.stdout)?,
        "| answer |\\n| --- |\\n"
    );
    Ok(())
}

#[test]
fn markdown_rejects_csv_only_no_header_flag() -> TestResult {
    let output = run("SELECT 1", &["--stdout-no-header"])?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("CSV or TSV"));
    Ok(())
}

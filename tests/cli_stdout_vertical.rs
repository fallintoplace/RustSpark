use std::process::{Command, Output};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(sql: &str, extra: &[&str]) -> std::io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", sql, "--stdout-format", "vertical"])
        .args(extra)
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
fn vertical_output_streams_rows_across_batches() -> TestResult {
    let output = run(
        "SELECT id, label FROM (VALUES (2, 'second'), (1, 'first')) AS t(id, label) ORDER BY id",
        &["--batch-size", "1"],
    )?;
    assert_success(&output);
    assert_eq!(
        String::from_utf8(output.stdout)?,
        "--- Row 1 ---\\nid: 1\\nlabel: first\\n--- Row 2 ---\\nid: 2\\nlabel: second\\n"
    );
    Ok(())
}

#[test]
fn vertical_output_renders_nulls_and_escapes_line_breaks() -> TestResult {
    let output = run("SELECT CAST(NULL AS VARCHAR) AS empty, 'a' AS value", &[])?;
    assert_success(&output);
    assert_eq!(
        String::from_utf8(output.stdout)?,
        "--- Row 1 ---\\nempty: NULL\\nvalue: a\\n"
    );
    Ok(())
}

#[test]
fn vertical_output_is_empty_when_query_has_no_rows() -> TestResult {
    let output = run("SELECT 42 AS answer WHERE FALSE", &[])?;
    assert_success(&output);
    assert_eq!(String::from_utf8(output.stdout)?, "");
    Ok(())
}

#[test]
fn vertical_output_rejects_headerless_csv_switch() -> TestResult {
    let output = run("SELECT 1", &["--stdout-no-header"])?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("CSV or TSV"));
    Ok(())
}

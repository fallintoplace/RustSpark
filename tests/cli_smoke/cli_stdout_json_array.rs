use std::process::{Command, Output};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(args: &[&str]) -> std::io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(args)
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
fn json_array_stdout_spans_batches_and_keeps_null_fields() -> TestResult {
    let output = run(&[
        "--batch-size",
        "1",
        "--sql",
        "SELECT 1 AS id, CAST(NULL AS VARCHAR) AS missing UNION ALL SELECT 2, 'yes' ORDER BY id",
        "--stdout-format",
        "json-array",
    ])?;
    assert_success(&output);
    assert_eq!(
        String::from_utf8(output.stdout)?,
        r#"[{"id":1,"missing":null},{"id":2,"missing":"yes"}]"#
    );
    Ok(())
}

#[test]
fn json_array_stdout_emits_empty_array_for_empty_query() -> TestResult {
    let output = run(&[
        "--sql",
        "SELECT 1 AS id WHERE FALSE",
        "--stdout-format",
        "json-array",
    ])?;
    assert_success(&output);
    assert_eq!(String::from_utf8(output.stdout)?, "[]");
    Ok(())
}

#[test]
fn json_array_stdout_respects_page_limit() -> TestResult {
    let output = run(&[
        "--sql",
        "SELECT n FROM (VALUES (1), (2), (3)) AS items(n) ORDER BY n",
        "--stdout-format",
        "json-array",
        "--max-rows",
        "1",
        "--offset",
        "1",
    ])?;
    assert_success(&output);
    assert_eq!(String::from_utf8(output.stdout)?, r#"[{"n":2}]"#);
    Ok(())
}

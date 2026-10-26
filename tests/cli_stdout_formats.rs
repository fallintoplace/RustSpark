use std::process::{Command, Output};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(args: &[&str]) -> std::io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(args)
        .output()
}

fn sales_csv() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn csv_stdout_streams_single_header_across_batches() -> TestResult {
    let output = run(&[
        "--csv",
        sales_csv(),
        "--batch-size",
        "1",
        "--sql",
        "SELECT city, amount FROM data ORDER BY amount",
        "--stdout-format",
        "csv",
    ])?;
    assert_success(&output);
    let stdout = String::from_utf8(output.stdout)?;
    let lines: Vec<_> = stdout.lines().collect();
    assert_eq!(lines, ["city,amount", "Munich,5", "Berlin,10", "Berlin,20"]);
    Ok(())
}

#[test]
fn csv_stdout_preserves_headers_for_empty_results() -> TestResult {
    let output = run(&[
        "--sql",
        "SELECT 42 AS answer WHERE FALSE",
        "--stdout-format",
        "csv",
    ])?;
    assert_success(&output);
    assert_eq!(String::from_utf8(output.stdout)?, "answer\n");
    Ok(())
}

#[test]
fn json_stdout_preserves_nulls_and_one_record_per_line() -> TestResult {
    let output = run(&[
        "--sql",
        "SELECT 1 AS id, CAST(NULL AS VARCHAR) AS missing UNION ALL SELECT 2, 'yes'",
        "--stdout-format",
        "json",
    ])?;
    assert_success(&output);
    let body = String::from_utf8(output.stdout)?;
    let lines: Vec<_> = body.lines().collect();
    assert_eq!(lines.len(), 2);
    assert!(lines.contains(&r#"{"id":1,"missing":null}"#), "{body}");
    assert!(lines.contains(&r#"{"id":2,"missing":"yes"}"#), "{body}");
    Ok(())
}

#[test]
fn streaming_stdout_supports_preview_pagination() -> TestResult {
    let output = run(&[
        "--csv",
        sales_csv(),
        "--sql",
        "SELECT city, amount FROM data ORDER BY amount",
        "--stdout-format",
        "csv",
        "--max-rows",
        "1",
        "--offset",
        "1",
    ])?;
    assert_success(&output);
    assert_eq!(
        String::from_utf8(output.stdout)?,
        "city,amount\nBerlin,10\n"
    );
    Ok(())
}

#[test]
fn stdout_format_conflicts_with_other_output_modes() -> TestResult {
    for extra in [
        ["--count-only", ""],
        ["--describe", ""],
        ["--output-json", "ignored"],
        ["--output-arrow", "ignored"],
    ] {
        let mut args = vec!["--sql", "SELECT 1", "--stdout-format", "json", extra[0]];
        if !extra[1].is_empty() {
            args.push(extra[1]);
        }
        let output = run(&args)?;
        assert!(!output.status.success(), "{}", extra[0]);
    }
    let unknown = run(&["--sql", "SELECT 1", "--stdout-format", "xml"])?;
    assert!(!unknown.status.success());
    Ok(())
}

#[cfg(unix)]
#[test]
fn closed_output_pipe_is_not_a_query_failure() -> TestResult {
    use std::process::Stdio;

    let mut child = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT 'hello' AS greeting",
            "--stdout-format",
            "csv",
        ])
        .stdout(Stdio::piped())
        .spawn()?;
    drop(child.stdout.take());
    assert!(child.wait()?.success());
    Ok(())
}

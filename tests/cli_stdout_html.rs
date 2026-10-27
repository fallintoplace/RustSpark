use std::process::{Command, Output};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(sql: &str, other_args: &[&str]) -> std::io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", sql, "--stdout-format", "html"])
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
fn html_streams_rows_across_batches() -> TestResult {
    let output = run(
        "SELECT id FROM (VALUES (3), (1), (2)) AS t(id) ORDER BY id",
        &["--batch-size", "1"],
    )?;
    assert_success(&output);
    assert_eq!(
        String::from_utf8(output.stdout)?,
        "<table>\n<thead><tr><th>id</th></tr></thead>\n<tbody>\n<tr><td>1</td></tr>\n<tr><td>2</td></tr>\n<tr><td>3</td></tr>\n</tbody></table>\n"
    );
    Ok(())
}

#[test]
fn html_escapes_text_and_distinguishes_null_values() -> TestResult {
    let output = run("SELECT 'it''s <&>\"' AS \"x&<\"", &[])?;
    assert_success(&output);
    let html = String::from_utf8(output.stdout)?;
    assert!(html.contains("<th>x&amp;&lt;</th>"), "{html}");
    assert!(
        html.contains("<td>it&#39;s &lt;&amp;&gt;&quot;</td>"),
        "{html}"
    );
    assert!(!html.contains("<&>"), "{html}");

    let null = run("SELECT CAST(NULL AS VARCHAR) AS missing", &[])?;
    assert_success(&null);
    let html = String::from_utf8(null.stdout)?;
    assert!(html.contains("<td class=\"null\">NULL</td>"), "{html}");
    Ok(())
}

#[test]
fn html_empty_result_has_headers_without_rows() -> TestResult {
    let output = run("SELECT 42 AS answer WHERE FALSE", &[])?;
    assert_success(&output);
    assert_eq!(
        String::from_utf8(output.stdout)?,
        "<table>\n<thead><tr><th>answer</th></tr></thead>\n<tbody>\n</tbody></table>\n"
    );
    Ok(())
}

#[test]
fn html_rejects_csv_only_no_header_flag() -> TestResult {
    let output = run("SELECT 1", &["--stdout-no-header"])?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("CSV or TSV"));
    Ok(())
}

#[test]
fn html_handles_plain_and_escaped_values_together() -> TestResult {
    let output = run("SELECT 'plain text' AS simple, 'a&b' AS encoded", &[])?;
    assert_success(&output);
    let html = String::from_utf8(output.stdout)?;
    assert!(html.contains("<th>simple</th><th>encoded</th>"), "{html}");
    assert!(html.contains("<td>plain text</td><td>a&amp;b</td>"), "{html}");
    Ok(())
}

use std::process::Command;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(sql: &str, extra_args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", sql])
        .args(extra_args)
        .output()
        .expect("rustspark starts")
}

#[test]
fn intermediate_select_rows_are_discarded_and_final_result_is_rendered() -> TestResult {
    let sql = "        WITH digits(n) AS (VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9))         SELECT a.n, b.n, c.n, d.n FROM digits a CROSS JOIN digits b CROSS JOIN digits c CROSS JOIN digits d;         SELECT 42 AS answer";
    let output = run(sql, &["--batch-size", "256", "--stdout-format", "csv"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8(output.stdout)?, "answer\n42\n");
    Ok(())
}

#[test]
fn intermediate_query_errors_still_fail_the_script() {
    let output = run(
        "SELECT CAST('not_an_integer' AS INT); SELECT 42 AS answer",
        &["--stdout-format", "csv"],
    );
    assert!(
        !output.status.success(),
        "execution errors must be propagated"
    );
    assert!(
        !String::from_utf8_lossy(&output.stdout).contains("42"),
        "final result must not be executed after intermediate failure"
    );
}

#[test]
fn intermediate_dml_still_executes_before_the_last_statement() -> TestResult {
    let db = tempfile::tempdir()?;
    let root = db.path().to_str().ok_or("temp path is not UTF-8")?;
    let output = run(
        "CREATE TABLE items (n BIGINT); INSERT INTO items VALUES (40), (2); SELECT SUM(n) AS total FROM items",
        &["--database-dir", root, "--stdout-format", "csv"],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8(output.stdout)?, "total\n42\n");
    Ok(())
}

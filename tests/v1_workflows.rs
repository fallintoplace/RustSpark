use std::process::{Command, Output};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn sales_csv() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")
}

fn run(args: &[&str]) -> std::io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(args)
        .output()
}

fn csv(output: Output) -> Result<String, Box<dyn std::error::Error>> {
    assert!(
        output.status.success(),
        "RustSpark failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

#[test]
fn v1_uses_typed_parameters_in_standalone_sql() -> TestResult {
    let result = csv(run(&[
        "--sql",
        "SELECT $1 + $2 AS answer",
        "--param",
        "int64:20",
        "--param",
        "int64:22",
        "--stdout-format",
        "csv",
    ])?)?;
    assert_eq!(result, "answer\n42\n");
    Ok(())
}

#[test]
fn v1_joins_named_input_files_and_groups_results() -> TestResult {
    let first = format!("first={}", sales_csv());
    let second = format!("second={}", sales_csv());
    let result = csv(run(&[
        "--csv-table",
        &first,
        "--csv-table",
        &second,
        "--sql",
        "SELECT city, COUNT(*) AS matches FROM first JOIN second USING(city) GROUP BY city ORDER BY city",
        "--stdout-format",
        "csv",
    ])?)?;
    assert_eq!(result, "city,matches\nBerlin,4\nMunich,1\n");
    Ok(())
}

#[test]
fn v1_persistent_sql_lifecycle_survives_process_restarts() -> TestResult {
    let directory = tempfile::tempdir()?;
    let root = directory.path().to_str().ok_or("UTF-8 temp directory required")?;
    let initial = csv(run(&[
        "--database-dir",
        root,
        "--sql",
        "CREATE TABLE events AS SELECT CAST(1 AS BIGINT) AS id; INSERT INTO events VALUES (2), (3); SELECT SUM(id) AS total FROM events",
        "--stdout-format",
        "csv",
    ])?)?;
    assert_eq!(initial, "total\n6\n");

    assert_eq!(
        csv(run(&[
            "--database-dir",
            root,
            "--sql",
            "SELECT id FROM events ORDER BY id",
            "--stdout-format",
            "csv",
        ])?)?,
        "id\n1\n2\n3\n"
    );

    let rename = run(&[
        "--database-dir",
        root,
        "--sql",
        "ALTER TABLE events RENAME TO history",
    ])?;
    assert!(
        rename.status.success(),
        "{}",
        String::from_utf8_lossy(&rename.stderr)
    );
    assert_eq!(
        csv(run(&[
            "--database-dir",
            root,
            "--sql",
            "SELECT SUM(id) AS total FROM history",
            "--stdout-format",
            "csv",
        ])?)?,
        "total\n6\n"
    );

    let drop = run(&["--database-dir", root, "--sql", "DROP TABLE history"])?;
    assert!(
        drop.status.success(),
        "{}",
        String::from_utf8_lossy(&drop.stderr)
    );
    assert!(!directory.path().join("history").exists());
    let missing = run(&["--database-dir", root, "--sql", "SELECT * FROM history"])?;
    assert!(!missing.status.success(), "DROP TABLE did not persist");
    Ok(())
}

#[test]
fn v1_reports_query_errors_and_still_starts_on_next_invocation() -> TestResult {
    let bad = run(&["--sql", "SELECT * FROM a_table_that_does_not_exist"])?;
    assert!(!bad.status.success());
    let good = csv(run(&[
        "--csv",
        sales_csv(),
        "--sql",
        "SELECT SUM(amount) AS total FROM data",
        "--stdout-format",
        "csv",
    ])?)?;
    assert_eq!(good, "total\n35\n");
    Ok(())
}

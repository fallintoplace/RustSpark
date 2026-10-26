use std::process::Command;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(args)
        .output()
        .expect("rustspark executes")
}

fn assert_row(output: std::process::Output, expected: &str) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout).expect("UTF-8 output");
    assert!(
        text.lines()
            .any(|line| line.trim().trim_matches('|').trim() == expected),
        "{text}"
    );
}

#[test]
fn saved_sql_table_is_queryable_from_a_new_process() -> TestResult {
    let dir = tempfile::tempdir()?;
    let root = dir.path().to_str().expect("UTF-8 temp path");
    let saved = run(&[
        "--database-dir", root,
        "--csv", "tests/fixtures/sales.csv",
        "--save-table", "totals",
        "--sql", "SELECT SUM(amount) AS total FROM data",
    ]);
    assert!(saved.status.success(), "{}", String::from_utf8_lossy(&saved.stderr));
    assert!(dir.path().join("totals").is_dir());

    assert_row(
        run(&["--database-dir", root, "--sql", "SELECT total FROM totals"]),
        "35",
    );

    let overwrite = run(&[
        "--database-dir", root,
        "--save-table", "totals",
        "--sql", "SELECT 900 AS total",
    ]);
    assert!(!overwrite.status.success());
    assert!(String::from_utf8_lossy(&overwrite.stderr).contains("already exists"));
    assert_row(
        run(&["--database-dir", root, "--sql", "SELECT total FROM totals"]),
        "35",
    );
    Ok(())
}

#[test]
fn detects_name_collision_with_loaded_tables() -> TestResult {
    let dir = tempfile::tempdir()?;
    let root = dir.path().to_str().expect("UTF-8 temp path");
    assert!(run(&[
        "--database-dir", root, "--save-table", "data", "--sql", "SELECT 1 AS n",
    ]).status.success());
    let conflict = run(&[
        "--database-dir", root,
        "--csv", "tests/fixtures/sales.csv",
        "--sql", "SELECT 1",
    ]);
    assert!(!conflict.status.success());
    assert!(String::from_utf8_lossy(&conflict.stderr).contains("conflicts with an input"));
    Ok(())
}

#[test]
fn skips_non_table_staging_directories_and_rejects_empty_table_directories() -> TestResult {
    let dir = tempfile::tempdir()?;
    std::fs::create_dir(dir.path().join(".rustspark-stage-incomplete"))?;
    let root = dir.path().to_str().expect("UTF-8 temp path");
    assert_row(run(&["--database-dir", root, "--sql", "SELECT 42"]), "42");
    std::fs::create_dir(dir.path().join("broken"))?;
    let invalid = run(&["--database-dir", root, "--sql", "SELECT 42"]);
    assert!(!invalid.status.success());
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("no Parquet files"));
    Ok(())
}

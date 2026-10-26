use std::process::Command;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(root: &str, sql: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--database-dir", root, "--sql", sql])
        .output()
        .expect("rustspark executes")
}

fn assert_row(output: std::process::Output, expected: &str) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 output");
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == expected),
        "{stdout}"
    );
}

#[test]
fn create_table_as_select_persists_between_processes() -> TestResult {
    let directory = tempfile::tempdir()?;
    let root = directory.path().to_str().expect("UTF-8 temp path");
    assert_row(
        run(root, "CREATE TABLE counts AS SELECT CAST(41 AS BIGINT) AS n; SELECT n + 1 FROM counts"),
        "42",
    );
    assert!(directory.path().join("counts").is_dir());
    assert_row(run(root, "SELECT n FROM counts"), "41");
    Ok(())
}

#[test]
fn insert_into_persistent_table_survives_restart() -> TestResult {
    let directory = tempfile::tempdir()?;
    let root = directory.path().to_str().expect("UTF-8 temp path");
    let created = run(root, "CREATE TABLE counts AS SELECT CAST(1 AS BIGINT) AS n");
    assert!(created.status.success(), "{}", String::from_utf8_lossy(&created.stderr));
    let inserted = run(root, "INSERT INTO counts (n) VALUES (2), (3)");
    assert!(inserted.status.success(), "{}", String::from_utf8_lossy(&inserted.stderr));
    assert_row(run(root, "SELECT SUM(n) FROM counts"), "6");
    Ok(())
}

#[test]
fn create_table_does_not_overwrite_an_existing_persistent_table() -> TestResult {
    let directory = tempfile::tempdir()?;
    let root = directory.path().to_str().expect("UTF-8 temp path");
    assert!(run(root, "CREATE TABLE counts AS SELECT 1 AS n").status.success());
    let duplicate = run(root, "CREATE TABLE counts AS SELECT 99 AS n");
    assert!(!duplicate.status.success());
    assert_row(run(root, "SELECT n FROM counts"), "1");
    Ok(())
}

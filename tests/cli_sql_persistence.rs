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
        run(
            root,
            "CREATE TABLE counts AS SELECT CAST(41 AS BIGINT) AS n; SELECT n + 1 FROM counts",
        ),
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
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    let inserted = run(root, "INSERT INTO counts (n) VALUES (2), (3)");
    assert!(
        inserted.status.success(),
        "{}",
        String::from_utf8_lossy(&inserted.stderr)
    );
    assert_row(run(root, "SELECT SUM(n) FROM counts"), "6");
    Ok(())
}

#[test]
fn create_table_does_not_overwrite_an_existing_persistent_table() -> TestResult {
    let directory = tempfile::tempdir()?;
    let root = directory.path().to_str().expect("UTF-8 temp path");
    assert!(
        run(root, "CREATE TABLE counts AS SELECT 1 AS n")
            .status
            .success()
    );
    let duplicate = run(root, "CREATE TABLE counts AS SELECT 99 AS n");
    assert!(!duplicate.status.success());
    assert_row(run(root, "SELECT n FROM counts"), "1");
    Ok(())
}

#[test]
fn unquoted_sql_table_names_are_case_insensitive() -> TestResult {
    let directory = tempfile::tempdir()?;
    let root = directory.path().to_str().expect("UTF-8 temp path");
    let created = run(root, "CREATE TABLE MIXEDCASE AS SELECT 9 AS n");
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    assert!(directory.path().join("mixedcase").is_dir());
    assert_row(run(root, "SELECT n FROM mixedcase"), "9");
    Ok(())
}

#[test]
fn empty_created_table_is_queryable_after_restart() -> TestResult {
    let directory = tempfile::tempdir()?;
    let root = directory.path().to_str().expect("UTF-8 temp path");
    let created = run(
        root,
        "CREATE TABLE empty_rows AS SELECT CAST(1 AS BIGINT) AS n WHERE FALSE",
    );
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    assert_row(run(root, "SELECT COUNT(*) FROM empty_rows"), "0");
    Ok(())
}

#[test]
fn empty_typed_table_survives_restart_and_accepts_inserts() -> TestResult {
    let directory = tempfile::tempdir()?;
    let root = directory.path().to_str().expect("UTF-8 temp path");
    let created = run(root, "CREATE TABLE items (id BIGINT, label VARCHAR)");
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    assert!(
        directory
            .path()
            .join("items")
            .join("part-0.parquet")
            .is_file()
    );
    assert_row(run(root, "SELECT COUNT(*) FROM items"), "0");

    let inserted = run(
        root,
        "INSERT INTO items (id, label) VALUES (1, 'first'), (2, 'second')",
    );
    assert!(
        inserted.status.success(),
        "{}",
        String::from_utf8_lossy(&inserted.stderr)
    );
    assert_row(run(root, "SELECT SUM(id) FROM items"), "3");
    Ok(())
}

#[test]
fn create_empty_table_if_not_exists_preserves_previous_rows() -> TestResult {
    let directory = tempfile::tempdir()?;
    let root = directory.path().to_str().expect("UTF-8 temp path");
    let created = run(root, "CREATE TABLE items (id BIGINT)");
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    let inserted = run(root, "INSERT INTO items (id) VALUES (41)");
    assert!(
        inserted.status.success(),
        "{}",
        String::from_utf8_lossy(&inserted.stderr)
    );

    let existing = run(root, "CREATE TABLE IF NOT EXISTS items (other INT)");
    assert!(
        existing.status.success(),
        "{}",
        String::from_utf8_lossy(&existing.stderr)
    );
    assert_row(run(root, "SELECT id FROM items"), "41");

    let duplicate = run(root, "CREATE TABLE items (id BIGINT)");
    assert!(!duplicate.status.success());
    assert_row(run(root, "SELECT id FROM items"), "41");
    Ok(())
}

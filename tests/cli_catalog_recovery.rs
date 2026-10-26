use std::{fs, process::Command};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(root: &str, sql: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--database-dir", root, "--sql", sql])
        .output()
        .expect("rustspark starts")
}

fn run_ok(root: &str, sql: &str) -> TestResult {
    let output = run(root, sql);
    if !output.status.success() {
        return Err(format!(
            "Query {sql:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(())
}

#[test]
fn incomplete_stages_are_removed_after_restart() -> TestResult {
    let db = tempfile::tempdir()?;
    let root = db.path().to_str().ok_or("non UTF-8 temp path")?;
    let staged = db.path().join(".rustspark-stage-incomplete");
    let dropped = db.path().join(".rustspark-drop-interrupted");
    fs::create_dir(&staged)?;
    fs::write(staged.join("orphan.parquet"), b"incomplete")?;
    fs::create_dir(&dropped)?;
    fs::write(dropped.join("old"), b"already dropped")?;
    let unrelated = db.path().join("user-notes.txt");
    fs::write(&unrelated, b"keep me")?;

    run_ok(root, "SELECT 42")?;
    assert!(!staged.exists());
    assert!(!dropped.exists());
    assert_eq!(fs::read(unrelated)?, b"keep me");
    Ok(())
}

#[test]
fn corrupted_persistent_parquet_rejected_at_startup() -> TestResult {
    let db = tempfile::tempdir()?;
    let root = db.path().to_str().ok_or("non UTF-8 temp path")?;
    run_ok(root, "CREATE TABLE items AS SELECT 1 AS id")?;
    fs::write(
        db.path().join("items").join("invalid.parquet"),
        b"not parquet",
    )?;

    let query = run(root, "SELECT 42 AS answer");
    assert!(!query.status.success());
    let stderr = String::from_utf8_lossy(&query.stderr);
    assert!(
        stderr.contains("Cannot read persistent Parquet file"),
        "{stderr}"
    );
    assert!(stderr.contains("invalid.parquet"), "{stderr}");
    Ok(())
}

#[test]
fn unknown_file_in_persistent_table_fails_closed() -> TestResult {
    let db = tempfile::tempdir()?;
    let root = db.path().to_str().ok_or("non UTF-8 temp path")?;
    run_ok(root, "CREATE TABLE items AS SELECT 1 AS id")?;
    fs::write(
        db.path().join("items").join("part.tmp"),
        b"interrupted write",
    )?;

    let result = run(root, "SELECT 42");
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("unexpected entry"));
    Ok(())
}

#[test]
fn valid_persistent_table_remains_queryable() -> TestResult {
    let db = tempfile::tempdir()?;
    let root = db.path().to_str().ok_or("non UTF-8 temp path")?;
    run_ok(root, "CREATE TABLE items AS SELECT 7 AS id")?;
    run_ok(root, "INSERT INTO items VALUES (8)")?;

    let result = run(root, "SELECT SUM(id) AS total FROM items");
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(String::from_utf8(result.stdout)?.contains("15"));
    Ok(())
}

use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(root: &Path, sql: &str, phase: Option<&str>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rustspark"));
    command
        .arg("--database-dir")
        .arg(root)
        .arg("--sql")
        .arg(sql);
    if let Some(phase) = phase {
        command.env("RUSTSPARK_TEST_INSERT_EXIT", phase);
    } else {
        command.env_remove("RUSTSPARK_TEST_INSERT_EXIT");
    }
    command.output().expect("rustspark starts")
}

fn run_ok(root: &Path, sql: &str) -> TestResult {
    let output = run(root, sql, None);
    if !output.status.success() {
        return Err(format!(
            "{sql:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(())
}

fn total(root: &Path) -> Result<i64, Box<dyn std::error::Error>> {
    let output = run(root, "SELECT SUM(id) FROM items", None);
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).to_string().into());
    }
    let stdout = String::from_utf8(output.stdout)?;
    // The CLI renders the single integer in a pretty table.
    let n = stdout
        .lines()
        .find_map(|line| line.trim().trim_matches('|').trim().parse::<i64>().ok())
        .ok_or_else(|| format!("No integer result in: {stdout}"))?;
    Ok(n)
}

#[test]
fn sql_error_discards_private_insert_and_preserves_original() -> TestResult {
    let db = tempfile::tempdir()?;
    run_ok(db.path(), "CREATE TABLE items (id BIGINT)")?;
    run_ok(db.path(), "INSERT INTO items VALUES (41)")?;
    let failed = run(
        db.path(),
        "INSERT INTO items VALUES ('not-an-integer')",
        None,
    );
    assert!(
        !failed.status.success(),
        "Invalid cast unexpectedly succeeded"
    );
    assert_eq!(total(db.path())?, 41);
    assert!(!db.path().join(".rustspark-insert-items").exists());
    Ok(())
}

#[test]
fn interrupted_insert_recovers_old_or_published_version() -> TestResult {
    // Each phase gets its own directory, so there is no test-order coupling.
    for (phase, expected) in [
        ("before_swap", 1_i64),
        ("after_old_moved", 1),
        ("after_publish", 3),
    ] {
        let db = tempfile::tempdir()?;
        run_ok(db.path(), "CREATE TABLE items (id BIGINT)")?;
        run_ok(db.path(), "INSERT INTO items VALUES (1)")?;

        let interrupted = run(db.path(), "INSERT INTO items VALUES (2)", Some(phase));
        assert_eq!(
            interrupted.status.code(),
            Some(97),
            "{phase}: {}",
            String::from_utf8_lossy(&interrupted.stderr)
        );
        let staging = db.path().join(".rustspark-insert-items");
        assert!(staging.exists(), "{phase} should leave a recoverable stage");
        assert_eq!(db.path().join("items").exists(), phase != "after_old_moved");

        // Opening a new process runs startup recovery while holding the lock.
        assert_eq!(total(db.path())?, expected, "phase {phase}");
        assert!(
            !staging.exists(),
            "{phase} recovery did not remove the stage"
        );
        assert_eq!(total(db.path())?, expected, "{phase}: repeated restart");
        assert!(fs::read_dir(db.path().join("items"))?.count() > 0);
    }
    Ok(())
}

#[test]
fn insert_select_from_same_table_is_published_once() -> TestResult {
    let db = tempfile::tempdir()?;
    run_ok(db.path(), "CREATE TABLE items (id BIGINT)")?;
    run_ok(db.path(), "INSERT INTO items VALUES (10)")?;
    run_ok(db.path(), "INSERT INTO items SELECT id + 1 FROM items")?;
    assert_eq!(total(db.path())?, 21);
    assert!(!db.path().join(".rustspark-insert-items").exists());
    Ok(())
}

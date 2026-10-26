use std::{
    fs,
    io::Write,
    path::Path,
    process::{Command, Output, Stdio},
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

#[test]
fn interactive_timeout_restores_live_table_provider() -> TestResult {
    let db = tempfile::tempdir()?;
    run_ok(db.path(), "CREATE TABLE items (id BIGINT)")?;
    run_ok(db.path(), "INSERT INTO items VALUES (41)")?;

    let mut child = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--database-dir")
        .arg(db.path())
        .arg("--interactive")
        .arg("--query-timeout")
        .arg("1")
        .env("RUSTSPARK_TEST_INSERT_PAUSE_MS", "1500")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    {
        let mut stdin = child.stdin.take().ok_or("missing shell stdin")?;
        writeln!(stdin, "INSERT INTO items VALUES (99);")?;
        writeln!(stdin, "SELECT SUM(id) FROM items;")?;
    }
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "The interactive shell should survive SQL errors"
    );
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("SQL query timed out"), "{stderr}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "41"),
        "{stdout}"
    );
    // A subsequent process discards the abandoned private stage.
    assert_eq!(total(db.path())?, 41);
    assert!(!db.path().join(".rustspark-insert-items").exists());
    Ok(())
}

#[test]
fn timed_out_insert_can_retry_in_same_interactive_session() -> TestResult {
    let db = tempfile::tempdir()?;
    run_ok(db.path(), "CREATE TABLE items (id BIGINT)")?;
    run_ok(db.path(), "INSERT INTO items VALUES (41)")?;

    let mut child = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--database-dir")
        .arg(db.path())
        .arg("--interactive")
        .arg("--query-timeout")
        .arg("1")
        .env("RUSTSPARK_TEST_INSERT_PAUSE_ONCE_MS", "1500")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    {
        let mut stdin = child.stdin.take().ok_or("missing shell stdin")?;
        writeln!(stdin, "INSERT INTO items VALUES (99);")?;
        writeln!(stdin, "INSERT INTO items VALUES (1);")?;
        writeln!(stdin, "SELECT SUM(id) FROM items;")?;
    }
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "interactive shell exited with error"
    );
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("SQL query timed out"), "{stderr}");
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "42"),
        "the retry did not commit: {stdout}"
    );
    assert_eq!(total(db.path())?, 42);
    assert!(!db.path().join(".rustspark-insert-items").exists());
    Ok(())
}

fn corrupt_parquet_part(directory: &Path) -> TestResult {
    let part = fs::read_dir(directory)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "parquet")
        })
        .ok_or("missing parquet part")?;
    fs::write(part, b"truncated parquet")?;
    Ok(())
}

#[test]
fn corrupt_published_snapshot_restores_valid_backup() -> TestResult {
    let db = tempfile::tempdir()?;
    run_ok(db.path(), "CREATE TABLE items (id BIGINT)")?;
    run_ok(db.path(), "INSERT INTO items VALUES (1)")?;
    let interrupted = run(
        db.path(),
        "INSERT INTO items VALUES (2)",
        Some("after_publish"),
    );
    assert_eq!(interrupted.status.code(), Some(97));

    // Only the newly appended part is independent of the previous snapshot.
    // The older immutable parts may share an inode through hard links.
    let previous: std::collections::HashSet<_> = fs::read_dir(
        db.path().join(".rustspark-insert-items").join("old"),
    )?
    .filter_map(Result::ok)
    .map(|entry| entry.file_name())
    .collect();
    let fresh_part = fs::read_dir(db.path().join("items"))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.extension().is_some_and(|extension| extension == "parquet")
                && !previous.contains(&path.file_name().unwrap().to_os_string())
        })
        .ok_or("missing freshly published Parquet part")?;
    fs::write(fresh_part, b"truncated parquet")?;
    assert_eq!(total(db.path())?, 1);
    assert!(!db.path().join(".rustspark-insert-items").exists());
    let quarantines: Vec<_> = fs::read_dir(db.path())?
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".rustspark-corrupt-")
        })
        .collect();
    assert_eq!(quarantines.len(), 1);
    assert!(quarantines[0].path().join("items").is_dir());
    assert_eq!(total(db.path())?, 1);
    Ok(())
}

#[test]
fn two_corrupt_snapshots_fail_closed_without_removing_backup() -> TestResult {
    let db = tempfile::tempdir()?;
    run_ok(db.path(), "CREATE TABLE items (id BIGINT)")?;
    run_ok(db.path(), "INSERT INTO items VALUES (1)")?;
    let interrupted = run(
        db.path(),
        "INSERT INTO items VALUES (2)",
        Some("after_publish"),
    );
    assert_eq!(interrupted.status.code(), Some(97));

    let stage = db.path().join(".rustspark-insert-items");
    corrupt_parquet_part(&db.path().join("items"))?;
    corrupt_parquet_part(&stage.join("old"))?;
    let failed = run(db.path(), "SELECT COUNT(*) FROM items", None);
    assert!(!failed.status.success());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("Both live and backup snapshots"));
    assert!(db.path().join("items").is_dir());
    assert!(stage.join("old").is_dir());
    Ok(())
}

#[test]
fn invalid_backup_between_swap_steps_is_not_promoted() -> TestResult {
    let db = tempfile::tempdir()?;
    run_ok(db.path(), "CREATE TABLE items (id BIGINT)")?;
    run_ok(db.path(), "INSERT INTO items VALUES (1)")?;
    let interrupted = run(
        db.path(),
        "INSERT INTO items VALUES (2)",
        Some("after_old_moved"),
    );
    assert_eq!(interrupted.status.code(), Some(97));

    let stage = db.path().join(".rustspark-insert-items");
    corrupt_parquet_part(&stage.join("old"))?;
    let failed = run(db.path(), "SELECT COUNT(*) FROM items", None);
    assert!(!failed.status.success());
    assert!(!db.path().join("items").exists());
    assert!(stage.join("old").is_dir());
    Ok(())
}

#[test]
fn failed_insert_publication_emits_no_success_result() -> TestResult {
    let db = tempfile::tempdir()?;
    run_ok(db.path(), "CREATE TABLE items (id BIGINT)")?;
    run_ok(db.path(), "INSERT INTO items VALUES (41)")?;

    let failed = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--database-dir")
        .arg(db.path())
        .arg("--sql")
        .arg("INSERT INTO items VALUES (99)")
        .env("RUSTSPARK_TEST_INSERT_COMMIT_ERROR", "before_swap")
        .output()?;
    assert!(!failed.status.success());
    let error = String::from_utf8_lossy(&failed.stderr);
    assert!(
        error.contains("Injected pre-publication INSERT failure"),
        "{error}"
    );
    assert!(
        failed.stdout.is_empty(),
        "failed INSERT printed success output"
    );
    assert_eq!(total(db.path())?, 41);
    assert!(!db.path().join(".rustspark-insert-items").exists());
    Ok(())
}

#[test]
fn successful_insert_outputs_single_result_after_commit() -> TestResult {
    let db = tempfile::tempdir()?;
    run_ok(db.path(), "CREATE TABLE items (id BIGINT)")?;
    let committed = run(db.path(), "INSERT INTO items VALUES (1)", None);
    assert!(committed.status.success());
    let stdout = String::from_utf8(committed.stdout)?;
    assert!(!stdout.is_empty(), "successful INSERT lost its result");
    assert_eq!(total(db.path())?, 1);
    Ok(())
}

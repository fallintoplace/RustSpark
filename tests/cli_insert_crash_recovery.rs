use std::{
    fs,
    process::{Command, Output},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(root: &str, sql: &str, failpoint: Option<&str>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rustspark"));
    command.args(["--database-dir", root, "--sql", sql, "--stdout-format", "csv"]);
    if let Some(point) = failpoint {
        command.env("RUSTSPARK_TEST_INSERT_EXIT", point);
    }
    command.output().expect("rustspark process starts")
}

fn assert_csv(root: &str, sql: &str, expected: &str) {
    let query = run(root, sql, None);
    assert!(
        query.status.success(),
        "{}",
        String::from_utf8_lossy(&query.stderr)
    );
    assert_eq!(String::from_utf8(query.stdout).expect("UTF-8 CSV"), expected);
}

#[cfg(debug_assertions)]
#[test]
fn interrupted_inserts_recover_to_either_complete_snapshot() -> TestResult {
    for (point, expected) in [
        ("after-stage", "id\n1\n"),
        ("after-backup", "id\n1\n"),
        ("after-publish", "id\n1\n2\n"),
    ] {
        let db = tempfile::tempdir()?;
        let root = db.path().to_str().ok_or("temporary path is not UTF-8")?;
        assert!(run(root, "CREATE TABLE items AS SELECT CAST(1 AS BIGINT) AS id", None).status.success());

        let crashed = run(root, "INSERT INTO items VALUES (2)", Some(point));
        assert_eq!(
            crashed.status.code(),
            Some(86),
            "failpoint {point} did not terminate: {}",
            String::from_utf8_lossy(&crashed.stderr)
        );
        let transaction = db.path().join(".rustspark-insert-items");
        assert!(transaction.exists(), "interrupted transaction missing for {point}");

        // A new process holds the DB lock while resolving the interrupted swap.
        assert_csv(root, "SELECT id FROM items ORDER BY id", expected);
        assert!(!transaction.exists(), "recovery left transaction for {point}");
    }
    Ok(())
}

#[cfg(debug_assertions)]
#[test]
fn corruption_after_publication_keeps_recovery_backup() -> TestResult {
    let db = tempfile::tempdir()?;
    let root = db.path().to_str().ok_or("temporary path is not UTF-8")?;
    assert!(run(root, "CREATE TABLE items AS SELECT CAST(1 AS BIGINT) AS id", None).status.success());
    let crashed = run(root, "INSERT INTO items VALUES (2)", Some("after-publish"));
    assert_eq!(crashed.status.code(), Some(86));

    let damaged = db.path().join("items").join("bad.parquet");
    fs::write(&damaged, b"invalid parquet")?;
    let rejected = run(root, "SELECT id FROM items", None);
    assert!(!rejected.status.success(), "corrupt published data must fail closed");
    let backup = db.path().join(".rustspark-insert-items").join("previous");
    assert!(backup.is_dir(), "previous snapshot was deleted on failed validation");

    fs::remove_file(damaged)?;
    assert_csv(root, "SELECT id FROM items ORDER BY id", "id\n1\n2\n");
    assert!(!db.path().join(".rustspark-insert-items").exists());
    Ok(())
}

#[cfg(debug_assertions)]
#[test]
fn unexpected_insert_journal_is_not_silently_deleted() -> TestResult {
    let db = tempfile::tempdir()?;
    let root = db.path().to_str().ok_or("temporary path is not UTF-8")?;
    let unknown = db.path().join(".rustspark-insert-missing");
    fs::create_dir(&unknown)?;
    fs::create_dir(unknown.join("next"))?;
    let result = run(root, "SELECT 42", None);
    assert!(!result.status.success());
    assert!(
        String::from_utf8_lossy(&result.stderr).contains("Missing original table during INSERT recovery")
    );
    assert!(unknown.is_dir());
    Ok(())
}

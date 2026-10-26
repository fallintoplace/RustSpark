use std::process::{Command, Output};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(root: &str, sql: &str, extra: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--database-dir", root, "--sql", sql])
        .args(extra)
        .output()
        .expect("rustspark starts")
}

fn csv(root: &str, sql: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = run(root, sql, &["--stdout-format", "csv"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

#[test]
fn insert_is_committed_after_complete_write_and_survives_restart() -> TestResult {
    let db = tempfile::tempdir()?;
    let root = db.path().to_str().ok_or("invalid test path")?;
    assert!(
        run(root, "CREATE TABLE records (id BIGINT)", &[])
            .status
            .success()
    );
    let inserted = run(
        root,
        "INSERT INTO records VALUES (1), (2)",
        &["--stdout-format", "csv"],
    );
    assert!(
        inserted.status.success(),
        "{}",
        String::from_utf8_lossy(&inserted.stderr)
    );
    assert_eq!(
        csv(root, "SELECT id FROM records ORDER BY id")?,
        "id\n1\n2\n"
    );
    assert!(!db.path().join(".rustspark-insert-records").exists());
    Ok(())
}

#[test]
fn failed_insert_preserves_the_previous_snapshot() -> TestResult {
    let db = tempfile::tempdir()?;
    let root = db.path().to_str().ok_or("invalid test path")?;
    assert!(
        run(
            root,
            "CREATE TABLE records AS SELECT CAST(7 AS BIGINT) AS id",
            &[]
        )
        .status
        .success()
    );
    let failed = run(root, "INSERT INTO records VALUES ('not an integer')", &[]);
    assert!(!failed.status.success());
    assert_eq!(csv(root, "SELECT id FROM records")?, "id\n7\n");
    assert!(!db.path().join(".rustspark-insert-records").exists());
    Ok(())
}

#[test]
fn insert_select_reads_old_rows_and_commits_new_snapshot() -> TestResult {
    let db = tempfile::tempdir()?;
    let root = db.path().to_str().ok_or("invalid test path")?;
    assert!(
        run(
            root,
            "CREATE TABLE records AS SELECT CAST(3 AS BIGINT) AS id",
            &[]
        )
        .status
        .success()
    );
    let appended = run(root, "INSERT INTO records SELECT id + 1 FROM records", &[]);
    assert!(
        appended.status.success(),
        "{}",
        String::from_utf8_lossy(&appended.stderr)
    );
    assert_eq!(
        csv(root, "SELECT id FROM records ORDER BY id")?,
        "id\n3\n4\n"
    );
    Ok(())
}

#[test]
fn persistent_insert_works_inside_multi_statement_scripts() -> TestResult {
    let db = tempfile::tempdir()?;
    let root = db.path().to_str().ok_or("invalid test path")?;
    assert_eq!(
        csv(
            root,
            "CREATE TABLE records (id BIGINT); INSERT INTO records VALUES (41); SELECT id + 1 AS answer FROM records"
        )?,
        "answer\n42\n"
    );
    Ok(())
}

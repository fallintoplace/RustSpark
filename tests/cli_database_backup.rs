use std::{fs, path::Path, process::Command};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn sql(root: &Path, query: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--database-dir")
        .arg(root)
        .arg("--sql")
        .arg(query)
        .args(["--stdout-format", "json"])
        .output()
        .expect("runs SQL")
}

fn backup(root: &Path, target: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--database-dir")
        .arg(root)
        .arg("--backup-database")
        .arg(target)
        .output()
        .expect("runs database backup")
}

fn assert_ok(output: std::process::Output) -> TestResult {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[test]
fn backup_copies_multiple_tables_and_survives_mutations() -> TestResult {
    let temp = tempfile::tempdir()?;
    let db = temp.path().join("db");
    let snapshot = temp.path().join("snapshot");
    assert_ok(sql(&db, "CREATE TABLE first AS SELECT 1 AS n"))?;
    assert_ok(sql(&db, "CREATE TABLE second AS SELECT 20 AS n"))?;
    assert_ok(sql(&db, "INSERT INTO first VALUES (2)"))?;
    assert_ok(backup(&db, &snapshot))?;
    assert!(snapshot.join("first").is_dir());
    assert!(snapshot.join("second").is_dir());
    let manifest = fs::read_to_string(snapshot.join(".rustspark-backup-manifest"))?;
    assert!(manifest.contains("rustspark-backup-v1"));
    assert!(manifest.contains("first"));
    assert!(manifest.contains("second"));
    assert_ok(sql(&db, "INSERT INTO first VALUES (100)"))?;

    let old = sql(&snapshot, "SELECT SUM(n) AS sum FROM first");
    assert!(
        old.status.success(),
        "{}",
        String::from_utf8_lossy(&old.stderr)
    );
    assert_eq!(String::from_utf8(old.stdout)?.trim(), r#"{"sum":3}"#);
    let new = sql(&db, "SELECT SUM(n) AS sum FROM first");
    assert_eq!(String::from_utf8(new.stdout)?.trim(), r#"{"sum":103}"#);
    Ok(())
}

#[test]
fn backup_rejects_nested_paths_and_existing_destinations() -> TestResult {
    let temp = tempfile::tempdir()?;
    let db = temp.path().join("db");
    assert_ok(sql(&db, "CREATE TABLE first AS SELECT 1 AS n"))?;
    let inside = db.join("inside");
    assert!(!backup(&db, &inside).status.success());
    assert!(!inside.exists());
    let outside = temp.path().join("outside");
    fs::create_dir(&outside)?;
    fs::write(outside.join("sentinel"), "retained")?;
    assert!(!backup(&db, &outside).status.success());
    assert_eq!(fs::read_to_string(outside.join("sentinel"))?, "retained");
    assert!(
        !fs::read_dir(temp.path())?
            .filter_map(Result::ok)
            .any(|item| item
                .file_name()
                .to_string_lossy()
                .starts_with(".rustspark-backup-stage-"))
    );
    Ok(())
}

#[test]
fn backups_include_empty_typed_tables() -> TestResult {
    let temp = tempfile::tempdir()?;
    let db = temp.path().join("db");
    let snapshot = temp.path().join("snapshot");
    assert_ok(sql(
        &db,
        "CREATE TABLE empty_rows (id BIGINT, label VARCHAR)",
    ))?;
    assert_ok(backup(&db, &snapshot))?;
    let row = sql(&snapshot, "SELECT COUNT(*) AS count FROM empty_rows");
    assert!(
        row.status.success(),
        "{}",
        String::from_utf8_lossy(&row.stderr)
    );
    assert_eq!(String::from_utf8(row.stdout)?.trim(), r#"{"count":0}"#);
    Ok(())
}

#[test]
fn backup_copies_many_independent_parquet_parts() -> TestResult {
    let temp = tempfile::tempdir()?;
    let db = temp.path().join("db");
    let snapshot = temp.path().join("snapshot");
    assert_ok(sql(&db, "CREATE TABLE events AS SELECT 7 AS value"))?;

    let directory = db.join("events");
    let original = fs::read_dir(&directory)?
        .next()
        .ok_or("missing Parquet part")??
        .path();
    for index in 1..=12 {
        fs::copy(
            &original,
            directory.join(format!("extra-{index:02}.parquet")),
        )?;
    }
    assert_ok(backup(&db, &snapshot))?;
    let output = sql(&snapshot, "SELECT COUNT(*) AS count FROM events");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8(output.stdout)?.trim(), r#"{"count":13}"#);
    Ok(())
}

#[cfg(unix)]
#[test]
fn backup_parts_do_not_share_inodes() -> TestResult {
    use std::os::unix::fs::MetadataExt;
    let temp = tempfile::tempdir()?;
    let db = temp.path().join("db");
    let snapshot = temp.path().join("copy");
    assert_ok(sql(&db, "CREATE TABLE data AS SELECT 1 AS n"))?;
    assert_ok(backup(&db, &snapshot))?;
    let source = fs::read_dir(db.join("data"))?
        .next()
        .ok_or("missing source")??
        .path();
    let copied = snapshot.join("data").join(source.file_name().unwrap());
    assert_ne!(fs::metadata(&source)?.ino(), fs::metadata(&copied)?.ino());
    Ok(())
}

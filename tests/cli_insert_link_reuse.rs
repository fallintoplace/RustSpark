use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run_ok(root: &Path, sql: &str) -> TestResult {
    let result = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--database-dir")
        .arg(root)
        .arg("--sql")
        .arg(sql)
        .output()?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    Ok(())
}

fn parquet_part(directory: &Path) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    Ok(fs::read_dir(directory)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "parquet")
        })
        .ok_or("no parquet part")?)
}

#[test]
fn repeated_inserts_leave_old_rows_intact() -> TestResult {
    let directory = tempfile::tempdir()?;
    run_ok(directory.path(), "CREATE TABLE numbers (n BIGINT)")?;
    for n in 1..=5 {
        run_ok(
            directory.path(),
            &format!("INSERT INTO numbers VALUES ({n})"),
        )?;
    }
    run_ok(
        directory.path(),
        "INSERT INTO numbers SELECT n + 10 FROM numbers WHERE n <= 2",
    )?;
    let result = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--database-dir")
        .arg(directory.path())
        .arg("--sql")
        .arg("SELECT SUM(n) AS total FROM numbers")
        .arg("--stdout-format")
        .arg("json")
        .output()?;
    assert!(result.status.success());
    assert_eq!(String::from_utf8(result.stdout)?.trim(), r#"{"total":38}"#);
    Ok(())
}

#[cfg(unix)]
#[test]
fn staged_insert_hard_links_immutable_parts() -> TestResult {
    use std::os::unix::fs::MetadataExt;

    let directory = tempfile::tempdir()?;
    let db = directory.path();
    run_ok(db, "CREATE TABLE numbers (n BIGINT)")?;
    run_ok(db, "INSERT INTO numbers VALUES (1)")?;
    let original = parquet_part(&db.join("numbers"))?;
    let original_content = fs::read(&original)?;

    let child = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--database-dir")
        .arg(db)
        .arg("--sql")
        .arg("INSERT INTO numbers VALUES (2)")
        .env("RUSTSPARK_TEST_INSERT_PAUSE_MS", "1500")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;

    let staged = db
        .join(".rustspark-insert-numbers")
        .join("new")
        .join(original.file_name().ok_or("missing file name")?);
    let deadline = Instant::now() + Duration::from_secs(8);
    while !staged.is_file() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(25));
    }
    if !staged.exists() {
        let output = child.wait_with_output()?;
        return Err(format!(
            "INSERT did not stage old part: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    let source = fs::metadata(&original)?;
    let linked = fs::metadata(&staged)?;
    let probe = db.join("hardlink-probe");
    let supported = fs::hard_link(&original, &probe).is_ok();
    let _ = fs::remove_file(&probe);
    if supported {
        assert_eq!(source.dev(), linked.dev());
        assert_eq!(
            source.ino(),
            linked.ino(),
            "expected metadata-only link reuse"
        );
    }
    let result = child.wait_with_output()?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        fs::read(db.join("numbers").join(original.file_name().unwrap()))?,
        original_content
    );
    Ok(())
}

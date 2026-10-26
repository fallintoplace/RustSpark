use std::{
    fs::{self, OpenOptions},
    process::Command,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(args)
        .output()
        .expect("rustspark process starts")
}

#[test]
fn database_lock_rejects_another_process_then_releases() -> TestResult {
    let dir = tempfile::tempdir()?;
    let root = dir.path().to_str().ok_or("non UTF-8 test path")?;
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.path().join(".rustspark.lock"))?;
    lock_file.lock()?;

    let blocked = run(&["--database-dir", root, "--sql", "SELECT 42 AS answer"]);
    assert!(!blocked.status.success());
    let stderr = String::from_utf8(blocked.stderr)?;
    assert!(
        stderr.contains("Cannot lock database directory"),
        "{stderr}"
    );
    drop(lock_file);

    let allowed = run(&["--database-dir", root, "--sql", "SELECT 42 AS answer"]);
    assert!(
        allowed.status.success(),
        "{}",
        String::from_utf8_lossy(&allowed.stderr)
    );
    assert!(String::from_utf8(allowed.stdout)?.contains("42"));
    Ok(())
}

#[test]
fn database_lock_survives_process_exit_without_staying_locked() -> TestResult {
    let dir = tempfile::tempdir()?;
    let root = dir.path().to_str().ok_or("non UTF-8 test path")?;
    for _ in 0..3 {
        let result = run(&["--database-dir", root, "--sql", "SELECT 1"]);
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    assert!(fs::metadata(dir.path().join(".rustspark.lock"))?.is_file());
    Ok(())
}

#[test]
fn local_queries_do_not_create_database_lock_files() -> TestResult {
    let dir = tempfile::tempdir()?;
    let result = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .current_dir(dir.path())
        .args(["--sql", "SELECT 1"])
        .output()?;
    assert!(result.status.success());
    assert!(!dir.path().join(".rustspark.lock").exists());
    Ok(())
}

#[test]
fn rejects_directory_at_database_lock_path() -> TestResult {
    let dir = tempfile::tempdir()?;
    fs::create_dir(dir.path().join(".rustspark.lock"))?;
    let root = dir.path().to_str().ok_or("non UTF-8 test path")?;
    let result = run(&["--database-dir", root, "--sql", "SELECT 42"]);
    assert!(!result.status.success());
    assert!(
        String::from_utf8_lossy(&result.stderr)
            .contains("Database lock path must be a regular file")
    );
    Ok(())
}

use std::process::Command;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn uses_selected_spill_directory_for_queries() -> TestResult {
    let scratch = tempfile::tempdir()?;
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            "tests/fixtures/sales.csv",
            "--memory-limit-mb",
            "64",
            "--sql",
            "SELECT amount FROM data ORDER BY amount DESC",
            "--stdout-format",
            "csv",
        ])
        .arg("--spill-dir")
        .arg(scratch.path())
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.starts_with("amount\n20\n"), "{stdout}");
    Ok(())
}

#[test]
fn rejects_missing_or_non_directory_spill_paths() -> TestResult {
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("not-a-directory");
    std::fs::write(&file, "text")?;
    for path in [file, dir.path().join("does-not-exist")] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--sql", "SELECT 1", "--spill-dir"])
            .arg(path)
            .output()?;
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("--spill-dir"));
    }
    Ok(())
}

#[test]
fn refuses_to_put_spill_files_in_persistent_database() -> TestResult {
    let root = tempfile::tempdir()?;
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 1", "--database-dir"])
        .arg(root.path())
        .arg("--spill-dir")
        .arg(root.path())
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("inside --database-dir"));
    Ok(())
}

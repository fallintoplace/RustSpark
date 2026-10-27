use std::process::Command;

#[test]
fn backup_and_compaction_flags_cannot_silently_skip_compaction()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let destination = root.path().join("snapshot");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--database-dir")
        .arg(root.path())
        .args(["--compact-table", "items"])
        .arg("--backup-database")
        .arg(&destination)
        .output()?;
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("cannot be used with"), "{stderr}");
    assert!(!destination.exists());
    Ok(())
}

#[cfg(unix)]
#[test]
fn save_table_rejects_dangling_symlink_without_replacing_it() -> Result<(), Box<dyn std::error::Error>> {
    use std::{fs, os::unix::fs::symlink, process::Command};

    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let destination = root.join("sales");
    let missing_target = root.join("does-not-exist");
    symlink(&missing_target, &destination)?;
    assert!(!destination.exists());

    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--database-dir")
        .arg(root)
        .args(["--save-table", "sales", "--sql", "SELECT 42 AS value"])
        .output()?;

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("already exists"));
    assert!(fs::symlink_metadata(&destination)?.file_type().is_symlink());
    assert_eq!(fs::read_link(&destination)?, missing_target);
    Ok(())
}

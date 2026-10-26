use std::process::Command;

#[test]
fn describe_and_explain_cannot_drop_persistent_tables() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let created = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--database-dir")
        .arg(root)
        .args(["--sql", "CREATE TABLE protected AS SELECT 42 AS n"])
        .output()?;
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );

    for flag in ["--describe", "--explain", "--explain-analyze"] {
        let inspected = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .arg("--database-dir")
            .arg(root)
            .args(["--sql", "DROP TABLE protected", flag])
            .output()?;
        assert!(
            !inspected.status.success(),
            "{flag} must not execute DROP TABLE"
        );
        assert!(root.join("protected").is_dir(), "{flag} deleted the table");

        let script = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .arg("--database-dir")
            .arg(root)
            .args(["--sql", "DROP TABLE protected; SELECT 1 AS n", flag])
            .output()?;
        assert!(
            !script.status.success(),
            "{flag} must reject mutating setup SQL"
        );
        assert!(root.join("protected").is_dir(), "{flag} deleted the table");
    }

    let checked = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--database-dir")
        .arg(root)
        .args(["--sql", "SELECT n FROM protected"])
        .output()?;
    assert!(
        checked.status.success(),
        "{}",
        String::from_utf8_lossy(&checked.stderr)
    );
    assert!(String::from_utf8_lossy(&checked.stdout).contains("42"));
    Ok(())
}

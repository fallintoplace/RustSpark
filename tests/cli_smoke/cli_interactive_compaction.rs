use std::{
    fs,
    io::Write,
    process::{Command, Stdio},
};

#[test]
fn compact_table_is_not_silently_skipped_in_interactive_mode()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    for statement in [
        "CREATE TABLE items (n BIGINT)",
        "INSERT INTO items VALUES (2)",
        "INSERT INTO items VALUES (3)",
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .arg("--database-dir")
            .arg(root.path())
            .args(["--sql", statement])
            .output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let items = root.path().join("items");
    assert!(fs::read_dir(&items)?.count() > 1);
    let mut child = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--database-dir")
        .arg(root.path())
        .args(["--compact-table", "items", "--interactive"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or("missing stdin")?
        .write_all(b"SELECT SUM(n) AS total FROM items;\n\\q\n")?;
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains('5'));
    assert_eq!(fs::read_dir(&items)?.count(), 1);
    Ok(())
}

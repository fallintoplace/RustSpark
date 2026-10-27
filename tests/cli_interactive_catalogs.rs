use std::{
    io::Write,
    process::{Command, Stdio},
};

#[test]
fn shell_lists_catalogs_and_still_executes_sql() -> Result<(), Box<dyn std::error::Error>> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--interactive", "--csv", "tests/fixtures/sales.csv"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or("missing stdin")?
        .write_all(b".catalogs\nSELECT 17 AS answer;\n.quit\n")?;
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert_eq!(stdout.lines().filter(|line| *line == "datafusion").count(), 1, "{stdout}");
    assert!(stdout.contains("17"), "{stdout}");
    assert!(!String::from_utf8_lossy(&output.stderr).contains("SQL error"));
    Ok(())
}

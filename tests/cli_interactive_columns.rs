use std::{
    io::Write,
    process::{Command, Stdio},
};

#[test]
fn shell_lists_columns_and_handles_missing_tables() -> Result<(), Box<dyn std::error::Error>> {
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
        .write_all(
            b".columns data\n.columns missing\n.columns\nSELECT 19 AS answer;\n.quit\n",
        )?;
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.lines().any(|line| line == "city"), "{stdout}");
    assert!(stdout.lines().any(|line| line == "amount"), "{stdout}");
    assert!(stdout.contains("19"), "{stdout}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("Shell error:"), "{stderr}");
    assert!(stderr.contains(".columns requires exactly one table name"), "{stderr}");
    Ok(())
}

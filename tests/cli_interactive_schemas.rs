use std::{
    io::Write,
    process::{Command, Stdio},
};

#[test]
fn shell_lists_schemas_and_continues_running_sql() -> Result<(), Box<dyn std::error::Error>> {
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
        .write_all(b".schemas\n.help\nSELECT 9 AS answer;\n\\q\n")?;
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.lines().any(|line| line == "datafusion.public"), "{stdout}");
    assert!(stdout.contains('9'), "{stdout}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains(".schemas"), "{stderr}");
    assert!(!stderr.contains("SQL error"), "{stderr}");
    Ok(())
}

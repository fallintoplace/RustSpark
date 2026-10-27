use std::{
    io::Write,
    process::{Command, Stdio},
};

#[test]
fn cancel_discards_pending_sql_without_affecting_later_queries()
-> Result<(), Box<dyn std::error::Error>> {
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
        .write_all(b"SELECT 999 AS abandoned\n.cancel\nSELECT 23 AS kept;\n.quit\n")?;
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(!stdout.contains("999"), "{stdout}");
    assert!(stdout.contains("23"), "{stdout}");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("Pending SQL cancelled"), "{stderr}");
    assert!(!stderr.contains("SQL error"), "{stderr}");
    Ok(())
}

#[test]
fn cancel_is_not_interpreted_inside_a_quoted_sql_literal() -> Result<(), Box<dyn std::error::Error>>
{
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
        .write_all(b"SELECT 'first\n.cancel\nlast' AS value;\n.quit\n")?;
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains(".cancel"), "{stdout}");
    assert!(!String::from_utf8_lossy(&output.stderr).contains("Pending SQL cancelled"));
    Ok(())
}

use std::{
    io::Write,
    process::{Command, Stdio},
};

#[test]
fn query_timer_can_be_enabled_and_disabled_between_statements()
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
        .write_all(b".timer\ton\nSELECT 17 AS first;\n.timer   off\nSELECT 19 AS second;\n.quit\n")?;
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("17"), "{stdout}");
    assert!(stdout.contains("19"), "{stdout}");
    let stderr = String::from_utf8(output.stderr)?;
    assert_eq!(stderr.matches("Time: ").count(), 1, "{stderr}");
    assert!(!stderr.contains("SQL error"), "{stderr}");
    Ok(())
}

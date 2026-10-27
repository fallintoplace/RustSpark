use std::{
    io::Write,
    process::{Command, Stdio},
};

#[test]
fn tabs_separate_interactive_table_commands() -> Result<(), Box<dyn std::error::Error>> {
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
        .write_all(b".columns\tdata\n.schema\tdata\n.count\tdata\n.quit\n")?;
    let output = child.wait_with_output()?;
    let stdout = String::from_utf8(output.stdout)?;
    let stderr = String::from_utf8(output.stderr)?;
    assert!(output.status.success(), "{stderr}");
    assert!(!stderr.contains("Shell error:"), "{stderr}");
    assert!(stdout.lines().any(|line| line == "city"), "{stdout}");
    assert!(
        stdout.lines().any(|line| line.starts_with("city\t")),
        "{stdout}"
    );
    assert!(stdout.lines().any(|line| line == "3"), "{stdout}");
    Ok(())
}

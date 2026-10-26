use std::{
    io::Write,
    process::{Command, Stdio},
};

#[test]
fn interactive_accepts_sql_with_utf8_bom() -> Result<(), Box<dyn std::error::Error>> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--interactive")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all("\u{feff}SELECT 42 AS answer;\n\\q\n".as_bytes())?;
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("42"));
    Ok(())
}

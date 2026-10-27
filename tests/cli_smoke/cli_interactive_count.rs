use std::{
    io::Write,
    process::{Command, Stdio},
};

#[test]
fn shell_counts_tables_without_exiting_on_errors() -> Result<(), Box<dyn std::error::Error>> {
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
            b".count data\n.count datafusion.public.data\n.count missing\n.count\n.count data extra\n.help\nSELECT 9 AS answer;\n\\q\n",
        )?;
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert_eq!(
        stdout.lines().filter(|line| line.trim() == "3").count(),
        2,
        "{stdout}"
    );
    assert!(stdout.contains('9'), "{stdout}");

    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains(".count TABLE"), "{stderr}");
    assert!(
        stderr.contains(".count requires exactly one table name"),
        "{stderr}"
    );
    assert!(stderr.contains("Shell error:"), "{stderr}");
    assert!(!stderr.contains("SQL error:"), "{stderr}");
    Ok(())
}

use std::{
    fs,
    io::Write,
    process::{Command, Stdio},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn shell(script: &str) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--interactive")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("starts shell");
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(script.as_bytes())
        .expect("writes shell input");
    child.wait_with_output().expect("shell exits")
}

#[test]
fn read_file_executes_multiple_statements_in_same_session() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("queries with spaces.sql");
    fs::write(
        &path,
        "\u{feff}CREATE VIEW from_file AS SELECT 13 AS n; SELECT 'semi;literal' AS word;",
    )?;
    let script = format!(".read {}\nSELECT n FROM from_file;\n\\q\n", path.display());
    let output = shell(&script);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("semi;literal"), "{stdout}");
    assert!(stdout.contains("13"), "{stdout}");
    assert!(!String::from_utf8_lossy(&output.stderr).contains("error:"));
    Ok(())
}

#[test]
fn read_errors_do_not_close_the_shell() -> TestResult {
    let dir = tempfile::tempdir()?;
    let bad = dir.path().join("bad.sql");
    fs::write(&bad, "SELECT * FROM;")?;
    let script = format!(
        ".read\n.read {}\n.read {}\nSELECT 42 AS after;\n\\q\n",
        dir.path().join("missing.sql").display(),
        bad.display(),
    );
    let output = shell(&script);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains(".read requires a file path"), "{stderr}");
    assert!(stderr.contains("Cannot read"), "{stderr}");
    assert!(stderr.contains("SQL error:"), "{stderr}");
    assert!(String::from_utf8_lossy(&output.stdout).contains("42"));
    Ok(())
}

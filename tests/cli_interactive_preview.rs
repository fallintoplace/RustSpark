use std::{
    io::Write,
    process::{Command, Stdio},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn shell(script: &str) -> TestResultOutput {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--interactive", "--csv", "tests/fixtures/sales.csv"])
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

type TestResultOutput = std::process::Output;

#[test]
fn previews_registered_tables_and_continues() -> TestResult {
    let output =
        shell(".preview data\n.preview datafusion.public.data 2\nSELECT 42 AS after;\n\\q\n");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("city"), "{stdout}");
    assert!(stdout.contains("amount"), "{stdout}");
    assert!(stdout.contains("Berlin"), "{stdout}");
    assert!(stdout.contains("42"), "{stdout}");
    assert!(!String::from_utf8_lossy(&output.stderr).contains("Shell error"));
    Ok(())
}

#[test]
fn preview_rejects_invalid_limits_and_unknown_tables_without_exiting() -> TestResult {
    let output = shell(
        ".preview\n.preview data 0\n.preview data 1001\n.preview data 1 extra\n.preview missing 2\nSELECT 17 AS after;\n\\q\n",
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.matches("Shell error:").count() >= 5, "{stderr}");
    assert!(String::from_utf8_lossy(&output.stdout).contains("17"));
    Ok(())
}

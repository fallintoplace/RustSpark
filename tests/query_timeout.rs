use std::process::Command;

#[test]
fn completes_query_with_timeout() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 42 AS answer", "--query-timeout", "30"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("42"));
}

#[test]
fn rejects_invalid_timeout() {
    for seconds in ["0", "-1", "none", "1.5"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--sql", "SELECT 1", "--query-timeout", seconds])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{seconds}");
    }
}

#[test]
fn query_timeout_includes_waiting_for_sql_stdin() -> Result<(), Box<dyn std::error::Error>> {
    use std::process::Stdio;
    let mut child = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql-file", "-", "--query-timeout", "1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    // Keep the input stream open to prove the deadline covers loading SQL.
    let _held_stdin = child.stdin.take();
    // Bound the regression test as well, so a timeout bug cannot hang CI.
    let started = std::time::Instant::now();
    while child.try_wait()?.is_none() {
        if started.elapsed() >= std::time::Duration::from_secs(10) {
            child.kill()?;
            child.wait()?;
            return Err("SQL stdin timeout test exceeded 10 seconds".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    let output = child.wait_with_output()?;
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("timed out"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

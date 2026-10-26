use std::process::Command;

#[test]
fn completes_query_with_timeout() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 42 AS answer", "--query-timeout", "30"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
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

use std::process::Command;

#[test]
fn queries_with_a_memory_limit() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            "tests/fixtures/sales.csv",
            "--memory-limit-mb",
            "64",
            "--sql",
            "SELECT SUM(amount) FROM data",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("35"));
    Ok(())
}

#[test]
fn rejects_invalid_memory_limits() -> Result<(), Box<dyn std::error::Error>> {
    for limit in ["0", "-1", "invalid", &usize::MAX.to_string()] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--sql", "SELECT 1", "--memory-limit-mb", limit])
            .output()?;
        assert!(!output.status.success(), "{limit}");
    }
    Ok(())
}

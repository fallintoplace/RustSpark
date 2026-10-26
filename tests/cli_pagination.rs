use std::process::Command;

fn sales_csv() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")
}

#[test]
fn paginates_sorted_query_results() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--sql",
            "SELECT amount FROM data ORDER BY amount DESC",
            "--max-rows",
            "1",
            "--offset",
            "1",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("| 10 |"), "{stdout}");
    assert!(!stdout.contains("| 20 |"), "{stdout}");
    assert!(!stdout.contains("| 5  |"), "{stdout}");
    Ok(())
}

#[test]
fn requires_a_page_size_for_offset() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 1", "--offset", "1"])
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--max-rows"));
    Ok(())
}

#[test]
fn rejects_overflowing_page_bounds() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT 1",
            "--max-rows",
            "1",
            "--offset",
            &usize::MAX.to_string(),
        ])
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("supported range"));
    Ok(())
}

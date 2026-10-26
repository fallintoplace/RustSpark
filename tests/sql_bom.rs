use std::process::Command;

#[test]
fn parses_utf8_bom_in_inline_sql() {
    let query = format!("{}SELECT 42 AS answer", '\u{feff}');
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", &query, "--stdout-format", "json"])
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
fn parses_utf8_bom_in_sql_files() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let script = directory.path().join("query.sql");
    std::fs::write(&script, format!("{}SELECT 23 AS answer", '\u{feff}'))?;
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--sql-file")
        .arg(&script)
        .args(["--stdout-format", "json"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("23"));
    Ok(())
}

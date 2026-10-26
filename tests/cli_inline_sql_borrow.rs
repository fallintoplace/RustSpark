use std::{fs, process::Command};

#[test]
fn inline_and_file_sql_produce_the_same_result() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let query = "SELECT 20 + 22 AS answer";
    let path = temporary.path().join("query.sql");
    fs::write(&path, query)?;
    let inline = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", query, "--stdout-format", "json"])
        .output()?;
    let file = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--sql-file").arg(&path)
        .args(["--stdout-format", "json"])
        .output()?;
    assert!(inline.status.success(), "{}", String::from_utf8_lossy(&inline.stderr));
    assert!(file.status.success(), "{}", String::from_utf8_lossy(&file.stderr));
    assert_eq!(inline.stdout, file.stdout);
    assert_eq!(String::from_utf8(inline.stdout)?.trim(), r#"{"answer":42}"#);
    Ok(())
}

use std::{fs, process::Command};

#[test]
fn refuses_csv_export_directory_containing_input_file() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let input = temp.path().join("sales.csv");
    fs::write(&input, "amount\n42\n")?;
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(&input)
        .args(["--sql", "SELECT amount FROM data", "--output-csv"])
        .arg(temp.path())
        .output()?;
    assert!(
        !output.status.success(),
        "export must not write next to its own source"
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("Output path must not overwrite"));
    assert_eq!(fs::read_to_string(&input)?, "amount\n42\n");
    Ok(())
}

#[test]
fn refuses_export_directory_containing_sql_script() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let script = temp.path().join("query.sql");
    fs::write(&script, "SELECT 42 AS answer;")?;
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--sql-file")
        .arg(&script)
        .arg("--output-json")
        .arg(temp.path())
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Output path must not overwrite"));
    assert_eq!(fs::read_to_string(script)?, "SELECT 42 AS answer;");
    Ok(())
}

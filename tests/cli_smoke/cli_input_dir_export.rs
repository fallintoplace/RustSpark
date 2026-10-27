use std::process::Command;

#[test]
fn refuses_output_inside_csv_input_directory() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let source = temporary.path().join("input");
    std::fs::create_dir(&source)?;
    std::fs::write(source.join("data.csv"), "value\n1\n")?;
    let output_path = source.join("result.arrow");

    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(&source)
        .args(["--sql", "SELECT * FROM data"])
        .arg("--output-arrow")
        .arg(&output_path)
        .output()?;

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("inside an input directory"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output_path.exists());
    Ok(())
}

#[test]
fn refuses_output_inside_named_input_directory() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let source = temporary.path().join("input");
    std::fs::create_dir(&source)?;
    let mapping = format!("events={}", source.display());
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--json-table", &mapping, "--sql", "SELECT 1"])
        .arg("--output-arrow")
        .arg(source.join("result.arrow"))
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("inside an input directory"));
    Ok(())
}

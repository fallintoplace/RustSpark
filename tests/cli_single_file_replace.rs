use std::{fs, process::Command};

#[test]
fn single_file_export_replaces_completed_output() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let destination = directory.path().join("result.csv");
    fs::write(&destination, "old contents")?;
    let result = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 42 AS answer", "--output-csv"])
        .arg(&destination)
        .arg("--output-single-file")
        .output()?;
    assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
    assert_eq!(fs::read_to_string(&destination)?, "answer\n42\n");
    assert!(!fs::read_dir(directory.path())?
        .filter_map(Result::ok)
        .any(|entry| entry.file_name().to_string_lossy().starts_with(".rustspark-export-")));
    Ok(())
}

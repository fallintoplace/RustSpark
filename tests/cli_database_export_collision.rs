use std::process::Command;

#[test]
fn rejects_exports_into_local_database_catalog() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("database");
    let file = root.join("report.arrow");
    let table = root.join("report");
    for (option, destination) in [
        ("--output-csv", root.as_path()),
        ("--output-json", table.as_path()),
        ("--output-parquet", table.as_path()),
        ("--output-arrow", file.as_path()),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .arg("--database-dir")
            .arg(&root)
            .args(["--sql", "SELECT 42 AS value"])
            .arg(option)
            .arg(destination)
            .output()?;
        assert!(
            !output.status.success(),
            "{option} must not write into the catalog"
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("inside --database-dir"),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert!(!table.exists());
    assert!(!file.exists());
    Ok(())
}

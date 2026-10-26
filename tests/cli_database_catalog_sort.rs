use std::{fs, process::Command};

#[test]
fn startup_registers_saved_tables_regardless_of_file_order() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("db");
    let created = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--database-dir").arg(&root)
        .args(["--sql", "CREATE TABLE zeta AS SELECT 4 AS n"])
        .output()?;
    assert!(created.status.success(), "{}", String::from_utf8_lossy(&created.stderr));
    let original = fs::read_dir(root.join("zeta"))?.next().ok_or("part missing")??.path();
    for table in ["omega", "alpha", "beta"] {
        let directory = root.join(table);
        fs::create_dir(&directory)?;
        fs::copy(&original, directory.join("part-0.parquet"))?;
    }
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--database-dir").arg(&root)
        .args(["--sql", "SELECT (SELECT SUM(n) FROM alpha) + (SELECT SUM(n) FROM zeta) AS value",
            "--stdout-format", "json"])
        .output()?;
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(String::from_utf8(output.stdout)?.trim(), r#"{"value":8}"#);
    Ok(())
}

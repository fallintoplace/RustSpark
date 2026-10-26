use std::{fs, process::Command};

#[test]
fn loads_many_validated_persistent_tables() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("db");
    let create = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--database-dir")
        .arg(&root)
        .args(["--sql", "CREATE TABLE source AS SELECT 3 AS value"])
        .output()?;
    assert!(
        create.status.success(),
        "{}",
        String::from_utf8_lossy(&create.stderr)
    );
    let original = fs::read_dir(root.join("source"))?
        .next()
        .ok_or("part missing")??
        .path();
    for idx in 0..12 {
        let table = root.join(format!("shard_{idx:02}"));
        fs::create_dir(&table)?;
        fs::copy(&original, table.join("part-0.parquet"))?;
    }
    let queries = (0..12)
        .map(|idx| format!("SELECT value FROM shard_{idx:02}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--database-dir")
        .arg(&root)
        .arg("--sql")
        .arg(format!("SELECT SUM(value) AS total FROM ({queries})"))
        .args(["--stdout-format", "json"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8(output.stdout)?.trim(), r#"{"total":36}"#);
    Ok(())
}

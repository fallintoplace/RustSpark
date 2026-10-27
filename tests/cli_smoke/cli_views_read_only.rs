use std::process::Command;

#[test]
fn cli_views_reject_mutating_sql() -> Result<(), Box<dyn std::error::Error>> {
    for option in ["--sql-view", "--materialized-sql-view"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .arg(option)
            .arg("unsafe=CREATE TABLE side_effect AS SELECT 1 AS n")
            .args(["--sql", "SELECT 42"])
            .output()?;
        assert!(
            !output.status.success(),
            "{option} should reject CREATE TABLE"
        );
    }
    Ok(())
}

#[test]
fn readonly_view_can_still_use_named_parameters() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql-view",
            "safe=SELECT $value AS n",
            "--named-param",
            "value=int64:42",
            "--sql",
            "SELECT n FROM safe",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("42"));
    Ok(())
}

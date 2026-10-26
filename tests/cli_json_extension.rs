use std::process::Command;

#[test]
fn reads_jsonl_files_with_custom_extension() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("events.jsonl");
    std::fs::write(&path, "{\"amount\":7}\n{\"amount\":8}\n")?;

    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--json")
        .arg(&path)
        .args([
            "--json-extension",
            ".jsonl",
            "--sql",
            "SELECT SUM(amount) AS total FROM data",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "15")
    );
    Ok(())
}

#[test]
fn accepts_jsonl_as_named_input() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("events.jsonl");
    std::fs::write(&path, "{\"id\":1}\n{\"id\":2}\n")?;
    let input = format!("events={}", path.display());

    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--json-table",
            &input,
            "--json-extension",
            ".jsonl",
            "--sql",
            "SELECT COUNT(*) FROM events",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("2"));
    Ok(())
}

#[test]
fn rejects_custom_extension_without_json_input() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 1", "--json-extension", ".jsonl"])
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires a JSON input"));
    Ok(())
}

#[test]
fn rejects_invalid_json_extension() -> Result<(), Box<dyn std::error::Error>> {
    for extension in ["jsonl", ".", ".json/l"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--sql", "SELECT 1", "--json-extension", extension])
            .output()?;
        assert!(!output.status.success(), "{extension}");
    }
    Ok(())
}

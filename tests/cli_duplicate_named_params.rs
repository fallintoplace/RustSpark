use std::process::Command;

#[test]
fn rejects_duplicate_parameters_before_registering_inputs() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing.csv");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(&missing)
        .args([
            "--named-param",
            "limit=int64:2",
            "--named-param",
            "limit=int64:3",
            "--sql",
            "SELECT 1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("Duplicate named parameter: limit"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

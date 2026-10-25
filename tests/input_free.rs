use std::process::Command;

#[test]
fn runs_a_literal_query_without_any_input_file() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 42 AS answer"])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("42"));
}

#[test]
fn runs_a_values_query_without_any_input_file() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT COUNT(*) FROM (VALUES (1), (2), (3)) AS v(n)",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("3"));
}

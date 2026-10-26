use std::process::Command;

#[test]
fn rejects_duplicate_union_destinations_before_planning_sources() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--union-by-name",
            "combined=first,second",
            "--union-by-name",
            "COMBINED=first,second",
            "--sql",
            "SELECT 1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("Duplicate union destination: COMBINED"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

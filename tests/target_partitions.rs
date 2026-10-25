use std::process::Command;

#[test]
fn configures_target_partitions_for_a_query() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            "tests/fixtures/sales.csv",
            "--target-partitions",
            "2",
            "--sql",
            "SELECT SUM(amount) FROM data",
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(String::from_utf8_lossy(&output.stdout).contains("35"));
}

#[test]
fn rejects_invalid_target_partition_counts() {
    for count in ["0", "-1", "invalid"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args([
                "--csv",
                "tests/fixtures/sales.csv",
                "--target-partitions",
                count,
                "--sql",
                "SELECT 1",
            ])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{count}");
    }
}

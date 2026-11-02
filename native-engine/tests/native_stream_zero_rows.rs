use std::process::Command;

#[test]
fn zero_output_cap_does_not_read_a_blocking_group_source() {
    let missing = std::env::temp_dir().join(format!(
        "rustspark-absent-source-{}-zero-rows.csv", std::process::id()));
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark-stream"))
        .args(["--csv", missing.to_str().unwrap(), "--table", "sales",
            "--schema", "region:text,amount:int",
            "--sql", "SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
            "--max-rows", "0"])
        .output().unwrap();
    assert!(output.status.success(), "{}",
        String::from_utf8_lossy(&output.stderr));
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "region,total\n");
}

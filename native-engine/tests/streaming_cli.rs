use std::{
    fs,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

fn fixture() -> std::path::PathBuf {
    let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let path = std::env::temp_dir().join(format!(
        "rustspark-native-stream-{}-{unique}.csv", std::process::id(),
    ));
    fs::write(&path, "city,amount\nBerlin,2\nParis,5\nRome,8\n").unwrap();
    path
}

#[test]
fn streams_typed_csv_with_a_filter_and_global_limit() {
    let path = fixture();
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark-native"))
        .args([
            "--stream", "--csv-schema", "city:text,amount:int64",
            "--csv", path.to_str().unwrap(), "--table", "sales",
            "--sql", "SELECT city FROM sales WHERE amount > 3 LIMIT 1",
        ])
        .output().unwrap();
    fs::remove_file(&path).unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "city\nParis\n");
}

#[test]
fn streaming_mode_rejects_blocking_operators() {
    let path = fixture();
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark-native"))
        .args([
            "--stream", "--csv", path.to_str().unwrap(), "--table", "sales",
            "--sql", "SELECT city, COUNT(*) FROM sales GROUP BY city",
        ])
        .output().unwrap();
    fs::remove_file(&path).unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unsupported"));
}

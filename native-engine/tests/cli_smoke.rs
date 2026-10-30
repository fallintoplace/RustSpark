use std::{
    fs,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

#[test]
fn native_cli_reads_csv_and_executes_grouped_sql() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "rustspark-native-smoke-{}-{unique}.csv",
        std::process::id()
    ));
    fs::write(&path, "city,amount\nBerlin,2\nBerlin,3\nParis,1\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark-native"))
        .args([
            "--csv",
            path.to_str().unwrap(),
            "--table",
            "sales",
            "--sql",
            "SELECT city, SUM(amount) AS total FROM sales GROUP BY city ORDER BY city",
        ])
        .output()
        .unwrap();
    fs::remove_file(&path).unwrap();
    assert!(
        output.status.success(),
        "native CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "city,total\nBerlin,5\nParis,1\n"
    );
}

#[test]
fn native_cli_named_csv_source_executes_sql() {
    let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let path = std::env::temp_dir().join(format!(
        "rustspark-native-named-source-{}-{unique}.csv", std::process::id()
    ));
    fs::write(&path, "id\\n1\\n2\\n").unwrap();
    let source = format!("sales={}", path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark-native"))
        .args(["--csv-table", &source, "--sql", "SELECT id FROM sales"])
        .output().unwrap();
    fs::remove_file(path).unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "id\\n1\\n2\\n");
}

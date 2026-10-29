use std::{
    fs,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

#[test]
fn default_rustspark_binary_uses_native_grouped_sql() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "rustspark-native-default-{}-{unique}.csv",
        std::process::id()
    ));
    fs::write(&path, "city,amount\nBerlin,2\nBerlin,3\nParis,1\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
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
fn unsupported_sql_does_not_use_a_datafusion_fallback() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "rustspark-no-fallback-{}-{unique}.csv",
        std::process::id()
    ));
    fs::write(&path, "v\n1\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--csv", path.to_str().unwrap(), "--sql", "SELECT v FROM data WINDOW w AS (PARTITION BY v)"])
        .output()
        .unwrap();
    fs::remove_file(path).unwrap();

    assert!(!output.status.success(), "unsupported SQL must fail closed");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("unsupported SQL"),
        "unexpected diagnostic: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

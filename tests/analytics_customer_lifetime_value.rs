use std::{path::Path, process::Command};

#[test]
fn customer_lifetime_value_analytics() {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/analytics/customer_lifetime_value");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(path.join("data.csv"))
        .args([
            "--table",
            "events",
            "--csv-schema",
            "customer_id:int64,region:utf8,order_id:int64,amount:int64",
            "--sql-file",
        ])
        .arg(path.join("query.sql"))
        .args(["--stdout-format", "json"])
        .output()
        .expect("run analytics example");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 output");
    assert_eq!(
        stdout.lines().collect::<Vec<_>>(),
        vec![
            r#"{"region":"eu","customers":2,"revenue":90,"highest_customer_value":50}"#,
            r#"{"region":"us","customers":2,"revenue":70,"highest_customer_value":60}"#,
        ]
    );
}

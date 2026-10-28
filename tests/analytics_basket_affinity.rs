use std::{path::Path, process::Command};

#[test]
fn basket_affinity_analytics() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/analytics/basket_affinity");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(path.join("data.csv"))
        .args([
            "--table",
            "events",
            "--csv-schema",
            "order_id:int64,product:utf8",
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
            r#"{"product_a":"bread","product_b":"butter","shared_orders":2}"#,
            r#"{"product_a":"bread","product_b":"milk","shared_orders":2}"#,
            r#"{"product_a":"butter","product_b":"milk","shared_orders":2}"#,
        ]
    );
}

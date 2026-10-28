use std::{path::Path, process::Command};

#[test]
fn inventory_running_balance_analytics() {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/analytics/inventory_running_balance");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(path.join("data.csv"))
        .args([
            "--table",
            "events",
            "--csv-schema",
            "sku:utf8,day:int64,delta:int64",
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
            r#"{"sku":"apple","day":1,"delta":10,"stock_on_hand":10}"#,
            r#"{"sku":"apple","day":2,"delta":-3,"stock_on_hand":7}"#,
            r#"{"sku":"apple","day":3,"delta":-8,"stock_on_hand":-1}"#,
            r#"{"sku":"banana","day":1,"delta":20,"stock_on_hand":20}"#,
            r#"{"sku":"banana","day":2,"delta":-5,"stock_on_hand":15}"#,
            r#"{"sku":"carrot","day":1,"delta":5,"stock_on_hand":5}"#,
            r#"{"sku":"carrot","day":2,"delta":-2,"stock_on_hand":3}"#,
            r#"{"sku":"carrot","day":3,"delta":1,"stock_on_hand":4}"#,
        ]
    );
}

use std::{path::Path, process::Command};

#[test]
fn top_products_analytics() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/analytics/top_products");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(path.join("data.csv"))
        .args([
            "--table",
            "events",
            "--csv-schema",
            "category:utf8,product:utf8,units:int64,unit_price:int64",
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
            r#"{"category":"books","product":"novel","revenue":90,"position":1}"#,
            r#"{"category":"books","product":"manual","revenue":80,"position":2}"#,
            r#"{"category":"games","product":"cards","revenue":60,"position":1}"#,
            r#"{"category":"games","product":"chess","revenue":60,"position":2}"#,
        ]
    );
}

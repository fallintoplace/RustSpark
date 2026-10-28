use std::{path::Path, process::Command};

#[test]
fn rfm_segments_analytics() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/analytics/rfm_segments");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(path.join("data.csv"))
        .args([
            "--table",
            "events",
            "--csv-schema",
            "customer_id:int64,day:int64,amount:int64",
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
            r#"{"customer_id":1,"days_since_purchase":2,"orders":2,"lifetime_revenue":30,"segment":"loyal"}"#,
            r#"{"customer_id":2,"days_since_purchase":30,"orders":1,"lifetime_revenue":100,"segment":"at_risk"}"#,
            r#"{"customer_id":3,"days_since_purchase":0,"orders":3,"lifetime_revenue":15,"segment":"loyal"}"#,
            r#"{"customer_id":4,"days_since_purchase":60,"orders":1,"lifetime_revenue":30,"segment":"at_risk"}"#,
        ]
    );
}

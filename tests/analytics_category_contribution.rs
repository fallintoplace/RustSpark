use std::{path::Path, process::Command};

#[test]
fn category_contribution_analytics() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/analytics/category_contribution");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(path.join("data.csv"))
        .args([
            "--table",
            "events",
            "--csv-schema",
            "category:utf8,units:int64,unit_price:int64",
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
            r#"{"category":"games","revenue":100,"overall_revenue":180,"revenue_rank":1}"#,
            r#"{"category":"books","revenue":50,"overall_revenue":180,"revenue_rank":2}"#,
            r#"{"category":"music","revenue":30,"overall_revenue":180,"revenue_rank":3}"#,
        ]
    );
}

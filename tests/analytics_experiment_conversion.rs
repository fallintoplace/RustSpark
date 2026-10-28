use std::{path::Path, process::Command};

#[test]
fn experiment_conversion_analytics() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/analytics/experiment_conversion");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(path.join("data.csv"))
        .args([
            "--table",
            "events",
            "--csv-schema",
            "variant:utf8,user_id:int64,converted:int64",
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
            r#"{"variant":"A","participants":4,"conversions":2,"non_conversions":2}"#,
            r#"{"variant":"B","participants":4,"conversions":3,"non_conversions":1}"#,
        ]
    );
}

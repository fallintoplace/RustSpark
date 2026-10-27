use std::{path::Path, process::Command};

#[test]
fn revenue_growth_analytics() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/analytics/revenue_growth");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(path.join("data.csv"))
        .args([
            "--table",
            "events",
            "--csv-schema",
            "month:utf8,amount:int64",
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
            r#"{"month":"2026-01","revenue":30,"previous_revenue":null,"change":null}"#,
            r#"{"month":"2026-02","revenue":50,"previous_revenue":30,"change":20}"#,
            r#"{"month":"2026-03","revenue":70,"previous_revenue":50,"change":20}"#,
        ]
    );
}

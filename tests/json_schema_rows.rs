use std::process::Command;

#[test]
fn limits_json_schema_inference_sample() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--json",
            "tests/fixtures/sales.json",
            "--json-schema-rows",
            "1",
            "--sql",
            "SELECT SUM(amount) FROM data",
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(String::from_utf8_lossy(&output.stdout).contains("35"));
}

#[test]
fn supports_named_json_table_schema_sample() {
    let table = format!("sales={}/tests/fixtures/sales.json", env!("CARGO_MANIFEST_DIR"));
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--json-table",
            table.as_str(),
            "--json-schema-rows",
            "1",
            "--sql",
            "SELECT SUM(amount) FROM sales",
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(String::from_utf8_lossy(&output.stdout).contains("35"));
}

#[test]
fn rejects_invalid_json_inference_row_count() {
    for count in ["0", "-1", "nan"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args([
                "--json",
                "tests/fixtures/sales.json",
                "--json-schema-rows",
                count,
                "--sql",
                "SELECT 1",
            ])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{count}");
    }
}

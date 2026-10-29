use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};
use std::process::Command;

#[test]
fn source_free_select_has_one_row_and_typed_expressions() {
    let catalog = Catalog::new();
    let cases = [
        ("SELECT 40 + 2 AS answer", vec![vec![Value::Integer(42)]]),
        ("SELECT 'Straße' AS city, TRUE AS active", vec![vec![Value::Text("Straße".into()), Value::Boolean(true)]]),
        ("SELECT NULL AS missing", vec![vec![Value::Null]]),
        ("SELECT 1 AS value WHERE FALSE", vec![]),
        ("SELECT COUNT(*) AS total", vec![vec![Value::Integer(1)]]),
        ("SELECT 9 AS value LIMIT 0", vec![]),
    ];
    for (sql, expected) in cases {
        assert_eq!(execute(&parse_sql(sql).unwrap(), &catalog).unwrap().rows().unwrap(), expected, "{sql}");
    }
    assert!(parse_sql("SELECT *").is_err());
    assert!(execute(&parse_sql("SELECT unknown").unwrap(), &catalog).is_err());
}

#[test]
fn cli_executes_sql_without_external_data_files() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark-native"))
        .args(["--sql", "SELECT 40 + 2 AS answer, UPPER('hello') AS word", "--jsonl"])
        .output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "{\"answer\":42,\"word\":\"HELLO\"}\n");
}

#[test]
fn streaming_still_requires_a_real_csv_source() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark-native"))
        .args(["--sql", "SELECT 1", "--stream"])
        .output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--stream requires a CSV source"));
}

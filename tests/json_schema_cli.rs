use std::process::Command;

fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn uses_explicit_json_schema_for_named_inputs() {
    let output = run(&[
        "--json-table",
        "events=tests/fixtures/sales.json",
        "--json-schema",
        "city:utf8,amount:int64",
        "--sql",
        "SELECT SUM(amount) FROM events",
    ]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("35"));
}

#[test]
fn explicit_json_schema_also_supports_json_arrays() {
    let output = run(&[
        "--json",
        "tests/fixtures/sales_array.json",
        "--json-array",
        "--json-schema",
        "city:string,amount:bigint",
        "--sql",
        "SELECT SUM(amount) FROM data",
    ]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("35"));
}

#[test]
fn rejects_invalid_json_schemas() {
    for schema in [
        "",
        "city",
        "city:missing",
        "city:utf8,CITY:int64",
        "123:int64",
    ] {
        let output = run(&[
            "--json",
            "tests/fixtures/sales.json",
            "--json-schema",
            schema,
            "--sql",
            "SELECT 1",
        ]);
        assert!(!output.status.success(), "{schema}");
    }
}

#[test]
fn rejects_json_schema_without_a_json_input() {
    let output = run(&["--sql", "SELECT 1", "--json-schema", "id:int64"]);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--json-schema requires a JSON input")
    );
}

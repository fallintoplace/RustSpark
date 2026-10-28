use std::{fs, process::Command};

#[test]
fn reads_typed_nested_json_columns() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let file = directory.path().join("nested.json");
    fs::write(
        &file,
        concat!(
            "{\"id\":1,\"tags\":[\"one\",\"two\"],\"profile\":{\"city\":\"Berlin\",\"level\":2},\"attrs\":{\"a\":10,\"b\":20}}\n",
            "{\"id\":2,\"tags\":[],\"profile\":{\"city\":\"Prague\",\"level\":null},\"attrs\":{\"a\":1}}\n",
        ),
    )?;
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--json")
        .arg(&file)
        .args([
            "--json-schema",
            "id:int64,tags:array<utf8>,profile:struct<city:utf8,level:int64>,attrs:map<utf8,int64>",
            "--sql",
            "SELECT id, tags, profile, attrs FROM data ORDER BY id",
            "--stdout-format",
            "json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert_eq!(stdout.lines().count(), 2);
    assert!(stdout.contains("one") && stdout.contains("two"), "{stdout}");
    assert!(
        stdout.contains("Berlin") && stdout.contains("Prague"),
        "{stdout}"
    );
    assert!(stdout.contains("attrs"), "{stdout}");
    Ok(())
}

#[test]
fn rejects_malformed_nested_schema_in_cli() {
    for schema in [
        "tags:array<>",
        "data:map<utf8>",
        "meta:struct<x:int64,X:int64>",
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args([
                "--json",
                "tests/fixtures/sales.json",
                "--json-schema",
                schema,
                "--sql",
                "SELECT 1",
            ])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{schema}");
    }
}

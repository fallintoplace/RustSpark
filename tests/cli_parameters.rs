use std::process::Command;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn sales_csv() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")
}

fn assert_result(output: std::process::Output, expected: &str) -> TestResult {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == expected),
        "{stdout}"
    );
    Ok(())
}

#[test]
fn binds_positional_parameters_without_sql_interpolation() -> TestResult {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--sql",
            "SELECT COUNT(*) FROM data WHERE amount > $1 AND city = $2",
            "--param",
            "int64:15",
            "--param",
            "utf8:Berlin",
        ])
        .output()?;
    assert_result(output, "1")?;

    let injection = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--sql",
            "SELECT COUNT(*) FROM data WHERE city = $1",
            "--param",
            "utf8:Berlin' OR 1=1 --",
        ])
        .output()?;
    assert_result(injection, "0")
}

#[test]
fn accepts_float_boolean_and_colon_in_string_values() -> TestResult {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT CASE WHEN $1 > 1.0 AND $2 THEN $3 ELSE 'no' END AS answer",
            "--param",
            "float64:2.5",
            "--param",
            "bool:true",
            "--param",
            "utf8:a:b",
        ])
        .output()?;
    assert_result(output, "a:b")
}

#[test]
fn rejects_invalid_typed_parameters() -> TestResult {
    for param in [
        "missing",
        "int64:not-a-number",
        "int64:9223372036854775808",
        "float64:NaN",
        "float64:inf",
        "bool:yes",
        "null:garbage",
        "unknown:1",
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--sql", "SELECT 1", "--param", param])
            .output()?;
        assert!(!output.status.success(), "{param}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("--param"),
            "{param}"
        );
    }
    Ok(())
}

#[test]
fn rejects_missing_values_without_substituting_sql_text() -> TestResult {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT $1 AS answer", "--param", "int64:3"])
        .output()?;
    assert_result(output, "3")
}

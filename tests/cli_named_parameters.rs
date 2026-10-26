use std::process::Command;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn invoke(args: &[&str]) -> Result<std::process::Output, std::io::Error> {
    Command::new(env!("CARGO_BIN_EXE_rustspark")).args(args).output()
}

fn assert_cell(output: std::process::Output, expected: &str) -> TestResult {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout)?;
    assert!(
        text.lines()
            .any(|line| line.trim().trim_matches('|').trim() == expected),
        "{text}"
    );
    Ok(())
}

#[test]
fn binds_named_values_repeatedly_without_sql_interpolation() -> TestResult {
    let csv = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv");
    assert_cell(
        invoke(&[
            "--csv", csv,
            "--sql", "SELECT COUNT(*) FROM data WHERE amount > $minimum AND city = $city",
            "--named-param", "city=utf8:Berlin",
            "--named-param", "minimum=int64:15",
        ])?,
        "1",
    )?;
    assert_cell(
        invoke(&[
            "--csv", csv,
            "--sql", "SELECT COUNT(*) FROM data WHERE city = $city",
            "--named-param", "city=utf8:Berlin' OR 1=1 --",
        ])?,
        "0",
    )?;
    assert_cell(
        invoke(&[
            "--sql", "SELECT $value || $value AS doubled",
            "--named-param", "value=utf8:a:b",
        ])?,
        "a:ba:b",
    )
}

#[test]
fn named_values_support_other_types() -> TestResult {
    assert_cell(
        invoke(&[
            "--sql", "SELECT CASE WHEN $x > 1.0 AND $ok THEN $message ELSE 'no' END",
            "--named-param", "x=float64:2.5",
            "--named-param", "ok=bool:true",
            "--named-param", "message=utf8:great",
        ])?,
        "great",
    )
}

#[test]
fn rejects_invalid_or_conflicting_parameters() -> TestResult {
    for bad in [
        "missing",
        "1bad=int64:3",
        "name=unknown:3",
        "name=int64:not-an-int",
        "name=float64:inf",
        "name=null:not-empty",
    ] {
        let output = invoke(&["--sql", "SELECT 1", "--named-param", bad])?;
        assert!(!output.status.success(), "{bad}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("--named-param"),
            "{bad}"
        );
    }

    let duplicate = invoke(&[
        "--sql", "SELECT $v", "--named-param", "v=int64:1",
        "--named-param", "v=int64:2",
    ])?;
    assert!(!duplicate.status.success());
    assert!(String::from_utf8_lossy(&duplicate.stderr).contains("Duplicate named parameter"));

    let mixed = invoke(&[
        "--sql", "SELECT 1", "--param", "int64:1",
        "--named-param", "v=int64:1",
    ])?;
    assert!(!mixed.status.success());
    Ok(())
}

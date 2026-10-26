use std::process::{Command, Output};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(args: &[&str]) -> std::io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(args)
        .output()
}

#[test]
fn materialized_views_use_inputs_and_existing_sql_views() -> TestResult {
    let output = run(&[
        "--csv",
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
        "--sql-view",
        "large=SELECT city, amount FROM data WHERE amount >= 10",
        "--materialized-sql-view",
        "totals=SELECT city, SUM(amount) AS total FROM large GROUP BY city",
        "--sql",
        "SELECT SUM(total) AS grand_total FROM totals",
    ])?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("30"));
    Ok(())
}

#[test]
fn materialized_views_can_reference_earlier_materialized_views() -> TestResult {
    let output = run(&[
        "--materialized-sql-view",
        "first=SELECT 42 AS value",
        "--materialized-sql-view",
        "second=SELECT value + 1 AS value FROM first",
        "--sql",
        "SELECT value FROM second",
    ])?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("43"));
    Ok(())
}

#[test]
fn rejects_bad_materialized_sql_view_declarations() -> TestResult {
    for definition in [
        "missing",
        "123=SELECT 1",
        "empty=",
        "empty=   ",
        "bad=SELECT * FROM unknown_table",
    ] {
        let output = run(&["--materialized-sql-view", definition, "--sql", "SELECT 1"])?;
        assert!(!output.status.success(), "{definition}");
    }
    Ok(())
}

#[test]
fn rejects_materialized_view_name_collisions() -> TestResult {
    let output = run(&[
        "--sql-view",
        "used=SELECT 42 AS value",
        "--materialized-sql-view",
        "used=SELECT 10 AS value",
        "--sql",
        "SELECT value FROM used",
    ])?;
    assert!(!output.status.success());
    Ok(())
}

#[test]
fn view_declarations_bind_named_parameters() -> TestResult {
    let output = run(&[
        "--csv",
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
        "--sql-view",
        "filtered=SELECT amount FROM data WHERE amount > $minimum",
        "--named-param",
        "minimum=int64:10",
        "--sql",
        "SELECT COUNT(*) FROM filtered",
    ])?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1"));
    Ok(())
}

#[test]
fn materialized_views_bind_positional_parameters() -> TestResult {
    let output = run(&[
        "--materialized-sql-view",
        "constant=SELECT $1 AS value",
        "--param",
        "int64:42",
        "--sql",
        "SELECT value FROM constant",
    ])?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("42"));
    Ok(())
}

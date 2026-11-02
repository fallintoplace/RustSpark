use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

fn run(query: &str) -> rustspark_native::Result<Value> {
    let plan = parse_sql(query)?;
    Ok(execute(&plan, &Catalog::new())?.row(0)?[0].clone())
}

#[test]
fn quarter_handles_calendar_boundaries_and_nulls() {
    for (date, expected) in [
        ("2024-01-01", 1), ("2024-03-31", 1), ("2024-04-01", 2),
        ("2024-06-30", 2), ("2024-07-01", 3), ("2024-09-30", 3),
        ("2024-10-01", 4), ("2024-12-31", 4), ("2024-02-29", 1),
    ] {
        assert_eq!(run(&format!("SELECT QUARTER('{date}')")).unwrap(), Value::Integer(expected));
    }
    assert_eq!(run("SELECT QUARTER(NULL)").unwrap(), Value::Null);
}

#[test]
fn quarter_validates_date_before_extracting_month() {
    assert!(run("SELECT QUARTER('2023-02-29')").is_err());
    assert!(run("SELECT QUARTER('2024-13-01')").is_err());
    assert!(run("SELECT QUARTER('invalid')").is_err());
    assert!(run("SELECT QUARTER(123)").is_err());
    assert!(run("SELECT QUARTER()").is_err());
}

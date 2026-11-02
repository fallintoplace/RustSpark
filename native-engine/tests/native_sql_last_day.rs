use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

fn run(query: &str) -> rustspark_native::Result<Value> {
    let plan = parse_sql(query)?;
    Ok(execute(&plan, &Catalog::new())?.row(0)?[0].clone())
}

#[test]
fn last_day_supports_leap_centuries_and_month_boundaries() {
    for (date, expected) in [
        ("2009-01-12", "2009-01-31"), ("2023-02-01", "2023-02-28"),
        ("2024-02-10", "2024-02-29"), ("1900-02-01", "1900-02-28"),
        ("2000-02-01", "2000-02-29"), ("2024-04-01", "2024-04-30"),
        ("2024-12-30", "2024-12-31"), ("9999-12-01", "9999-12-31"),
    ] {
        assert_eq!(run(&format!("SELECT LAST_DAY('{date}')")).unwrap(),
            Value::Text(expected.into()));
    }
    assert_eq!(run("SELECT LAST_DAY(NULL)").unwrap(), Value::Null);
}

#[test]
fn last_day_rejects_invalid_dates_and_types() {
    assert!(run("SELECT LAST_DAY('2023-02-29')").is_err());
    assert!(run("SELECT LAST_DAY('2024-02-30')").is_err());
    assert!(run("SELECT LAST_DAY('2024-13-01')").is_err());
    assert!(run("SELECT LAST_DAY(2009)").is_err());
    assert!(run("SELECT LAST_DAY()").is_err());
}

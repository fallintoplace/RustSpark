use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

fn run(query: &str) -> rustspark_native::Result<Value> {
    let plan = parse_sql(query)?;
    Ok(execute(&plan, &Catalog::new())?.row(0)?[0].clone())
}

#[test]
fn spark_weekday_counts_from_monday_zero() {
    for (date, expected) in [
        ("1969-12-28", 6), ("1969-12-29", 0), ("1970-01-01", 3),
        ("2024-03-03", 6), ("2024-03-04", 0), ("2024-03-09", 5),
        ("2024-02-29", 3),
    ] {
        assert_eq!(run(&format!("SELECT WEEKDAY('{date}')")).unwrap(),
            Value::Integer(expected));
    }
    assert_eq!(run("SELECT WEEKDAY(NULL)").unwrap(), Value::Null);
}

#[test]
fn weekday_validates_iso_calendar_and_type() {
    assert!(run("SELECT WEEKDAY('2023-02-29')").is_err());
    assert!(run("SELECT WEEKDAY('2024-04-31')").is_err());
    assert!(run("SELECT WEEKDAY(42)").is_err());
    assert!(run("SELECT WEEKDAY()").is_err());
}

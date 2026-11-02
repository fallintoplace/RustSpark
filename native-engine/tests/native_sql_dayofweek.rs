use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

fn run(query: &str) -> rustspark_native::Result<Value> {
    let plan = parse_sql(query)?;
    Ok(execute(&plan, &Catalog::new())?.row(0)?[0].clone())
}

#[test]
fn spark_dayofweek_counts_from_sunday() {
    for (date, expected) in [
        ("1969-12-28", 1), ("1969-12-29", 2), ("1970-01-01", 5),
        ("2024-03-03", 1), ("2024-03-04", 2), ("2024-03-09", 7),
        ("2024-02-29", 5),
    ] {
        assert_eq!(run(&format!("SELECT DAYOFWEEK('{date}')")).unwrap(),
            Value::Integer(expected));
    }
    assert_eq!(run("SELECT DAYOFWEEK(NULL)").unwrap(), Value::Null);
}

#[test]
fn dayofweek_rejects_invalid_gregorian_dates() {
    assert!(run("SELECT DAYOFWEEK('2023-02-29')").is_err());
    assert!(run("SELECT DAYOFWEEK('2024-11-31')").is_err());
    assert!(run("SELECT DAYOFWEEK(42)").is_err());
    assert!(run("SELECT DAYOFWEEK()").is_err());
}

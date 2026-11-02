use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

fn value(sql: &str) -> Value {
    let plan = parse_sql(sql).unwrap();
    execute(&plan, &Catalog::new()).unwrap().row(0).unwrap()[0].clone()
}

#[test]
fn ansi_trim_direction_and_character_set() {
    assert_eq!(value("SELECT TRIM(BOTH FROM '  Spark  ')"), Value::Text("Spark".into()));
    assert_eq!(value("SELECT TRIM(LEADING FROM '  Spark  ')"), Value::Text("Spark  ".into()));
    assert_eq!(value("SELECT TRIM(TRAILING FROM '  Spark  ')"), Value::Text("  Spark".into()));
    assert_eq!(value("SELECT TRIM('SL' FROM 'SSparkSQLS')"), Value::Text("parkSQ".into()));
    assert_eq!(value("SELECT TRIM(BOTH 'SL' FROM 'SSparkSQLS')"), Value::Text("parkSQ".into()));
    assert_eq!(value("SELECT TRIM(LEADING 'SL' FROM 'SSparkSQLS')"), Value::Text("parkSQLS".into()));
    assert_eq!(value("SELECT TRIM(TRAILING 'SL' FROM 'SSparkSQLS')"), Value::Text("SSparkSQ".into()));
}

#[test]
fn ordinary_trim_and_invalid_ansi_forms() {
    assert_eq!(value("SELECT TRIM('  x  ')"), Value::Text("x".into()));
    assert_eq!(value("SELECT TRIM('xxyxx', 'x')"), Value::Text("y".into()));
    assert_eq!(value("SELECT TRIM(BOTH FROM NULL)"), Value::Null);
    assert!(parse_sql("SELECT TRIM(LEADING 'x')").is_err());
    assert!(parse_sql("SELECT TRIM(TRAILING FROM)").is_err());
    assert!(parse_sql("SELECT TRIM(BOTH FROM 'x', 'y')").is_err());
}

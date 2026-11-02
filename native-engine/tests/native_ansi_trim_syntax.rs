use rustspark_native::{catalog::Catalog, executor, scalar::Value, sql::parse_sql};
fn rows(sql: &str) -> Vec<Vec<Value>> {
    executor::execute(&parse_sql(sql).unwrap(), &Catalog::new())
        .unwrap().rows().unwrap()
}

#[test]
fn ansi_trim_directional_forms_work_with_custom_characters() {
    assert_eq!(rows("SELECT TRIM(LEADING 'x' FROM 'xxabx'), TRIM(TRAILING 'x' FROM 'xxabx'), TRIM(BOTH 'x' FROM 'xxabx')"), vec![vec![
        Value::Text("abx".into()), Value::Text("xxab".into()), Value::Text("ab".into()),
    ]]);
}
#[test]
fn ansi_trim_default_spaces_and_standard_function_remain_usable() {
    assert_eq!(rows("SELECT TRIM(LEADING FROM '  hi  '), TRIM(BOTH FROM '  hi  '), TRIM('  hi  ')"), vec![vec![
        Value::Text("hi  ".into()), Value::Text("hi".into()), Value::Text("hi".into()),
    ]]);
    assert!(parse_sql("SELECT TRIM(LEADING 'x' 'xx')").is_err());
}

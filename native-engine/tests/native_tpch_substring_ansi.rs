use rustspark_native::{
    catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql,
};

fn result(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &Catalog::new())
        .unwrap().rows().unwrap()
}

#[test]
fn tpch_q22_substring_supports_from_for() {
    assert_eq!(
        result("SELECT SUBSTRING('13123456789' FROM 1 FOR 2) AS country_code"),
        vec![vec![Value::Text("13".into())]],
    );
    assert_eq!(
        result("SELECT SUBSTRING('12345' FROM 3) AS suffix"),
        vec![vec![Value::Text("345".into())]],
    );
}

#[test]
fn substring_comma_form_keeps_working() {
    assert_eq!(
        result("SELECT SUBSTRING('abcdef', 2, 3)"),
        vec![vec![Value::Text("bcd".into())]],
    );
    assert!(parse_sql("SELECT SUBSTRING('abcdef' FROM 2 FOR)").is_err());
}

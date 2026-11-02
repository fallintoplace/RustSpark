use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &Catalog::new()).unwrap().rows().unwrap()
}

#[test]
fn make_date_validates_gregorian_calendar() {
    assert_eq!(rows("SELECT MAKE_DATE(2024, 2, 29)"), vec![vec![Value::Text("2024-02-29".into())]]);
    assert_eq!(rows("SELECT MAKE_DATE(2000, 2, 29)"), vec![vec![Value::Text("2000-02-29".into())]]);
    assert_eq!(rows("SELECT MAKE_DATE(2019, 7, NULL)"), vec![vec![Value::Null]]);
    for q in ["SELECT MAKE_DATE(2023, 2, 29)", "SELECT MAKE_DATE(1900, 2, 29)",
              "SELECT MAKE_DATE(0, 1, 1)", "SELECT MAKE_DATE(2024, 13, 1)",
              "SELECT MAKE_DATE(2024, 1)"] {
        let plan = parse_sql(q).unwrap();
        assert!(execute(&plan, &Catalog::new()).is_err());
    }
}

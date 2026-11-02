use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};
fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &Catalog::new()).unwrap().rows().unwrap()
}

#[test]
fn date_part_year_accepts_case_insensitive_field_and_null() {
    assert_eq!(rows("SELECT DATE_PART('year', DATE '2024-02-29')"),
        vec![vec![Value::Integer(2024)]]);
    assert_eq!(rows("SELECT DATEPART('YEAR', '1994-04-01')"),
        vec![vec![Value::Integer(1994)]]);
    assert_eq!(rows("SELECT DATE_PART('YEAR', NULL)"), vec![vec![Value::Null]]);
    for q in ["SELECT DATE_PART('MONTH', '2024-01-01')",
              "SELECT DATEPART('YEAR', 'not-a-date')",
              "SELECT DATE_PART('YEAR', '2024-01-01', 3)"] {
        let plan = parse_sql(q).unwrap();
        assert!(execute(&plan, &Catalog::new()).is_err());
    }
}

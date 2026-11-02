use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &Catalog::new()).unwrap().rows().unwrap()
}

#[test]
fn min_max_distinct_match_the_regular_extrema() {
    assert_eq!(rows("SELECT MIN(DISTINCT col1), MAX(DISTINCT col1) FROM VALUES (3), (1), (3), (NULL)"),
        vec![vec![Value::Integer(1), Value::Integer(3)]]);
    assert_eq!(rows("SELECT MIN(col1), MAX(col1) FROM VALUES (3), (1), (3), (NULL)"),
        rows("SELECT MIN(DISTINCT col1), MAX(DISTINCT col1) FROM VALUES (3), (1), (3), (NULL)"));
}

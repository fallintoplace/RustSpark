use rustspark_native::{catalog::Catalog, executor, scalar::Value, sql::parse_sql};
#[test]
fn zero_argument_math_constants_are_typed_doubles() {
    let catalog = Catalog::new();
    let result = executor::execute(&parse_sql("SELECT PI(), E()").unwrap(), &catalog)
        .unwrap().rows().unwrap();
    assert_eq!(result, vec![vec![
        Value::Float(std::f64::consts::PI),
        Value::Float(std::f64::consts::E),
    ]]);
}
#[test]
fn math_constants_reject_arguments() {
    for sql in ["SELECT PI(1)", "SELECT E(NULL)"] {
        assert!(executor::execute(&parse_sql(sql).unwrap(), &Catalog::new()).is_err());
    }
}

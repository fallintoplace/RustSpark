use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn run(sql: &str) -> Vec<Vec<Value>> {
    let mut catalog = Catalog::new();
    catalog.register("customer", Batch::from_rows(
        Schema::new(vec![Field::new("c_phone", DataType::Text, false)]).unwrap(),
        vec![
            vec![Value::Text("13-555-1234".into())],
            vec![Value::Text("31-555-6789".into())],
        ],
    ).unwrap()).unwrap();
    execute(&parse_sql(sql).unwrap(), &catalog).unwrap().rows().unwrap()
}

#[test]
fn tpch_q22_substring_from_for_extracts_country_code() {
    let query = "SELECT SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode FROM customer ORDER BY cntrycode";
    assert_eq!(run(query), vec![
        vec![Value::Text("13".into())],
        vec![Value::Text("31".into())],
    ]);
}

#[test]
fn substring_from_without_for_is_allowed() {
    assert_eq!(
        run("SELECT SUBSTRING(c_phone FROM 4) AS suffix FROM customer"),
        vec![
            vec![Value::Text("555-1234".into())],
            vec![Value::Text("555-6789".into())],
        ],
    );
}

#[test]
fn comma_form_still_works() {
    assert_eq!(
        run("SELECT SUBSTRING(c_phone, 1, 2) AS code FROM customer"),
        vec![
            vec![Value::Text("13".into())],
            vec![Value::Text("31".into())],
        ],
    );
}

#[test]
fn malformed_standard_substring_is_rejected() {
    assert!(parse_sql("SELECT SUBSTRING(c_phone FROM 1 FOR ) FROM customer").is_err());
}

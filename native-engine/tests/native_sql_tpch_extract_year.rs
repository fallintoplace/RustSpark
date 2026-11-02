use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn execute_sql(query: &str) -> rustspark_native::Result<Vec<Vec<Value>>> {
    let mut catalog = Catalog::new();
    catalog.register("lineitem", Batch::from_rows(
        Schema::new(vec![Field::new("l_shipdate", DataType::Text, true)]).unwrap(),
        vec![
            vec![Value::Text("1995-02-28".into())],
            vec![Value::Text("1996-02-29".into())],
            vec![Value::Null],
        ],
    ).unwrap()).unwrap();
    Ok(execute(&parse_sql(query)?, &catalog)?.rows()?)
}

#[test]
fn tpch_extract_year_from_iso_dates() {
    assert_eq!(
        execute_sql("SELECT EXTRACT(year FROM l_shipdate) AS l_year FROM lineitem").unwrap(),
        vec![vec![Value::Integer(1995)], vec![Value::Integer(1996)], vec![Value::Null]],
    );
}

#[test]
fn year_function_matches_extract() {
    assert_eq!(
        execute_sql("SELECT YEAR(l_shipdate) AS l_year FROM lineitem").unwrap(),
        execute_sql("SELECT EXTRACT(YEAR FROM l_shipdate) AS l_year FROM lineitem").unwrap(),
    );
}

#[test]
fn invalid_dates_and_unsupported_extract_fields_fail() {
    assert!(parse_sql("SELECT EXTRACT(century FROM l_shipdate) FROM lineitem").is_err());
    assert!(execute_sql("SELECT YEAR('1995-02-29') FROM lineitem").is_err());
}

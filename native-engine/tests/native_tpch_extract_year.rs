use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn rows(sql: &str) -> Vec<Vec<Value>> {
    let mut catalog = Catalog::new();
    catalog.register("lineitem", Batch::from_rows(
        Schema::new(vec![Field::new("l_shipdate", DataType::Text, true)]).unwrap(),
        vec![
            vec![Value::Text("1995-03-15".into())],
            vec![Value::Text("1996-02-29".into())],
            vec![Value::Null],
        ],
    ).unwrap()).unwrap();
    execute(&parse_sql(sql).unwrap(), &catalog).unwrap().rows().unwrap()
}

#[test]
fn tpch_year_extraction_from_iso_date_columns() {
    assert_eq!(
        rows("SELECT EXTRACT(YEAR FROM l_shipdate) FROM lineitem ORDER BY l_shipdate NULLS LAST"),
        vec![
            vec![Value::Integer(1995)],
            vec![Value::Integer(1996)],
            vec![Value::Null],
        ],
    );
    assert_eq!(
        rows("SELECT YEAR('1996-02-29')"),
        vec![vec![Value::Integer(1996)]],
    );
}

#[test]
fn invalid_dates_and_unsupported_parts_fail_closed() {
    assert!(parse_sql("SELECT EXTRACT(MONTH FROM l_shipdate) FROM lineitem").is_err());
    assert!(execute(
        &parse_sql("SELECT YEAR('1995-02-29')").unwrap(), &Catalog::new(),
    ).is_err());
    assert!(execute(
        &parse_sql("SELECT YEAR('1995-13-01')").unwrap(), &Catalog::new(),
    ).is_err());
}

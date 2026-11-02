use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let mut c = Catalog::new();
    c.register("lineitem", Batch::from_rows(
        Schema::new(vec![Field::new("l_shipdate", DataType::Text, false)]).unwrap(),
        vec![
            vec![Value::Text("1994-01-01".into())],
            vec![Value::Text("1994-12-31".into())],
            vec![Value::Text("1995-01-01".into())],
        ],
    ).unwrap()).unwrap();
    c
}

#[test]
fn tpch_date_literals_compare_to_iso_text_columns() {
    let plan = parse_sql(
        "SELECT l_shipdate FROM lineitem WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' ORDER BY l_shipdate"
    ).unwrap();
    assert_eq!(execute(&plan, &catalog()).unwrap().rows().unwrap(), vec![
        vec![Value::Text("1994-01-01".into())],
        vec![Value::Text("1994-12-31".into())],
    ]);
}

#[test]
fn date_literal_validates_leap_years_and_format() {
    assert!(parse_sql("SELECT DATE '2000-02-29'").is_ok());
    for date in ["1900-02-29", "1995-02-30", "1995-13-01", "0000-01-01", "1995-1-01", "1995-01-00"] {
        assert!(parse_sql(&format!("SELECT DATE '{date}'")).is_err(), "{date}");
    }
}

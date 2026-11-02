use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let mut c = Catalog::new();
    c.register("lineitem", Batch::from_rows(
        Schema::new(vec![Field::new("l_shipdate", DataType::Text, false)]).unwrap(),
        vec![
            vec![Value::Text("1998-09-01".into())],
            vec![Value::Text("1998-09-02".into())],
            vec![Value::Text("1998-12-02".into())],
        ],
    ).unwrap()).unwrap();
    c
}

#[test]
fn tpch_q1_date_minus_day_interval_filters_correctly() {
    let query = "SELECT l_shipdate FROM lineitem WHERE l_shipdate <= '1998-12-01' - INTERVAL '90' DAY ORDER BY l_shipdate";
    assert_eq!(execute(&parse_sql(query).unwrap(), &catalog()).unwrap().rows().unwrap(), vec![
        vec![Value::Text("1998-09-01".into())],
        vec![Value::Text("1998-09-02".into())],
    ]);
    assert_eq!(
        execute(&parse_sql("SELECT '1999-12-31' + INTERVAL '1' DAY").unwrap(), &Catalog::new()).unwrap().rows().unwrap(),
        vec![vec![Value::Text("2000-01-01".into())]],
    );
}

#[test]
fn unsupported_interval_units_and_non_integer_days_fail_closed() {
    for sql in [
        "SELECT '1998-12-01' + INTERVAL '3' MONTH",
        "SELECT '1998-12-01' + INTERVAL '1.5' DAY",
        "SELECT '1998-12-01' - INTERVAL 'oops' DAY",
    ] {
        assert!(parse_sql(sql).is_err(), "{sql}");
    }
}

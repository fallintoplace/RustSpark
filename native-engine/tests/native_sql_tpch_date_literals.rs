use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn run(query: &str) -> rustspark_native::Result<Vec<Vec<Value>>> {
    let mut catalog = Catalog::new();
    catalog.register("lineitem", Batch::from_rows(
        Schema::new(vec![Field::new("l_shipdate", DataType::Text, true)]).unwrap(),
        vec![vec![Value::Text("1995-09-01".into())],
             vec![Value::Text("1995-10-01".into())], vec![Value::Null]],
    ).unwrap()).unwrap();
    Ok(execute(&parse_sql(query)?, &catalog)?.rows()?)
}

#[test]
fn tpch_q14_standard_date_literal_filters_iso_text() {
    assert_eq!(
        run("SELECT l_shipdate FROM lineitem WHERE l_shipdate >= DATE '1995-09-01' AND l_shipdate < CAST('1995-10-01' AS DATE)").unwrap(),
        vec![vec![Value::Text("1995-09-01".into())]],
    );
}

#[test]
fn date_cast_and_literal_share_validation() {
    assert_eq!(
        run("SELECT DATE '1996-02-29' AS d FROM lineitem").unwrap().len(), 3,
    );
    for query in [
        "SELECT DATE '1995-02-29' FROM lineitem",
        "SELECT CAST('1995-02-29' AS DATE) FROM lineitem",
        "SELECT DATE '1995-13-01' FROM lineitem",
    ] {
        assert!(run(query).is_err(), "{query}");
    }
}

#[test]
fn date_cast_preserves_nulls() {
    assert_eq!(
        run("SELECT CAST(l_shipdate AS DATE) FROM lineitem").unwrap(),
        vec![
            vec![Value::Text("1995-09-01".into())],
            vec![Value::Text("1995-10-01".into())],
            vec![Value::Null],
        ],
    );
}

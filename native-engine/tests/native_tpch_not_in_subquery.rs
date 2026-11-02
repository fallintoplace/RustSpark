use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog(keys: Vec<Option<i64>>) -> Catalog {
    let mut catalog = Catalog::new();
    catalog.register("parts", Batch::from_rows(
        Schema::new(vec![Field::new("p_partkey", DataType::Integer, true)]).unwrap(),
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(2)],
            vec![Value::Null],
        ],
    ).unwrap()).unwrap();
    catalog.register("partsupp", Batch::from_rows(
        Schema::new(vec![Field::new("ps_partkey", DataType::Integer, true)]).unwrap(),
        keys.into_iter().map(|key| vec![key.map(Value::Integer)
            .unwrap_or(Value::Null)]).collect(),
    ).unwrap()).unwrap();
    catalog
}

fn rows(catalog: &Catalog, where_clause: &str) -> Vec<Vec<Value>> {
    let sql = format!(
        "SELECT p_partkey FROM parts WHERE p_partkey NOT IN (SELECT ps_partkey FROM partsupp {where_clause}) ORDER BY p_partkey NULLS LAST"
    );
    execute(&parse_sql(&sql).unwrap(), catalog).unwrap().rows().unwrap()
}

#[test]
fn tpch_q16_not_in_subquery_obeys_sql_null_semantics() {
    assert_eq!(rows(&catalog(vec![Some(2), Some(3)]), ""), vec![
        vec![Value::Integer(1)],
    ]);
    assert_eq!(rows(&catalog(vec![Some(2), None]), ""), Vec::<Vec<Value>>::new());
    assert_eq!(rows(&catalog(vec![]), ""), vec![
        vec![Value::Integer(1)],
        vec![Value::Integer(2)],
        vec![Value::Integer(2)],
        vec![Value::Null],
    ]);
}

#[test]
fn not_in_subquery_ignores_nulls_only_when_no_rows_are_returned() {
    assert_eq!(rows(&catalog(vec![Some(2), None]), "WHERE ps_partkey > 50"), vec![
        vec![Value::Integer(1)],
        vec![Value::Integer(2)],
        vec![Value::Integer(2)],
        vec![Value::Null],
    ]);
}

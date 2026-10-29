use rustspark_native::{
    catalog::Catalog, executor::execute, plan::Plan, scalar::Value,
    sql::parse_sql,
};

fn result(sql: &str) -> rustspark_native::batch::Batch {
    execute(&parse_sql(sql).unwrap(), &Catalog::new()).unwrap()
}

#[test]
fn standalone_values_returns_multiple_typed_rows() {
    let rows = result("VALUES (1, 'x'), (2, 'y'), (NULL, 'z')").rows().unwrap();
    assert_eq!(rows, vec![
        vec![Value::Integer(1), Value::Text("x".into())],
        vec![Value::Integer(2), Value::Text("y".into())],
        vec![Value::Null, Value::Text("z".into())],
    ]);
}

#[test]
fn from_values_aliases_join_projection_filter_and_order() {
    let query = "SELECT id, label FROM VALUES (2, 'b'), (1, 'a'), (3, 'c') AS data(id, label) WHERE id > 1 ORDER BY id";
    let data = result(query);
    assert_eq!(data.rows().unwrap(), vec![
        vec![Value::Integer(2), Value::Text("b".into())],
        vec![Value::Integer(3), Value::Text("c".into())],
    ]);
    assert_eq!(data.schema().fields()[0].name, "id");
}

#[test]
fn values_promotes_integers_to_doubles_without_losing_row_types() {
    let data = result("VALUES (1, NULL), (2.5, 'x')");
    assert_eq!(data.rows().unwrap(), vec![
        vec![Value::Float(1.0), Value::Null],
        vec![Value::Float(2.5), Value::Text("x".into())],
    ]);
}

#[test]
fn all_null_column_is_nullable_and_uses_text_fallback() {
    let data = result("VALUES (NULL), (NULL)");
    assert_eq!(data.num_rows(), 2);
    assert_eq!(data.schema().fields()[0].name, "col1");
    assert!(data.schema().fields()[0].nullable);
}

#[test]
fn computed_values_are_evaluated_and_type_checked() {
    let data = result("SELECT col1 FROM VALUES (2 + 3), (4 * 5)");
    assert_eq!(data.rows().unwrap(), vec![
        vec![Value::Integer(5)], vec![Value::Integer(20)],
    ]);
    assert!(parse_sql("VALUES (1, 2), (3)").is_err());
    assert!(parse_sql("VALUES (1) AS t(a,b)").is_err());
    assert!(parse_sql("VALUES (1, 2) AS t(a, A)").is_err());
    assert!(parse_sql("VALUES (1), (2) AS t(x) extra").is_err());
    let incompatible = parse_sql("VALUES (1), ('not a number')").unwrap();
    assert!(execute(&incompatible, &Catalog::new()).is_err());
}

#[test]
fn inline_values_have_an_explicit_owned_logical_plan() {
    assert!(matches!(parse_sql("VALUES (1)").unwrap(), Plan::Values { columns, rows }
        if columns == vec!["col1"] && rows.len() == 1));
}

use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn rows(sql: &str) -> rustspark_native::Result<Vec<Vec<Value>>> {
    let mut catalog = Catalog::new();
    let orders_schema = Schema::new(vec![
        Field::new("o_orderkey", DataType::Integer, true),
        Field::new("o_orderstatus", DataType::Text, false),
    ]).unwrap();
    catalog.register("orders", Batch::from_rows(orders_schema, vec![
        vec![Value::Integer(1), Value::Text("F".into())],
        vec![Value::Integer(2), Value::Text("O".into())],
        vec![Value::Integer(3), Value::Text("F".into())],
        vec![Value::Null, Value::Text("F".into())],
    ]).unwrap()).unwrap();
    let items_schema = Schema::new(vec![
        Field::new("l_orderkey", DataType::Integer, true),
        Field::new("l_quantity", DataType::Integer, true),
    ]).unwrap();
    catalog.register("lineitem", Batch::from_rows(items_schema, vec![
        vec![Value::Integer(1), Value::Integer(3)],
        vec![Value::Integer(1), Value::Integer(5)],
        vec![Value::Integer(2), Value::Integer(7)],
        vec![Value::Null, Value::Integer(10)],
    ]).unwrap()).unwrap();
    Ok(execute(&parse_sql(sql)?, &catalog)?.rows()?)
}

#[test]
fn tpch_q18_positive_in_subquery_preserves_outer_cardinality() {
    let sql = "SELECT o_orderkey FROM orders
        WHERE o_orderkey IN (
          SELECT l_orderkey FROM lineitem GROUP BY l_orderkey
          HAVING SUM(l_quantity) > 6
        ) AND o_orderstatus = 'F' ORDER BY o_orderkey";
    assert_eq!(rows(sql).unwrap(), vec![vec![Value::Integer(1)]]);
}

#[test]
fn in_subquery_must_project_one_column() {
    assert!(parse_sql("SELECT o_orderkey FROM orders WHERE o_orderkey IN
        (SELECT l_orderkey, l_quantity FROM lineitem)").is_err());
}

#[test]
fn regular_in_expression_is_unchanged() {
    assert_eq!(rows("SELECT o_orderkey FROM orders WHERE o_orderkey IN (1,3) ORDER BY o_orderkey").unwrap(),
               vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

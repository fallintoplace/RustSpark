use rustspark_native::{batch::Batch, catalog::Catalog, executor::execute, scalar::Value, schema::{DataType, Field, Schema}, sql::parse_sql};

#[test]
fn coalesce_uses_first_nonnull_value_without_evaluating_fallback() {
    let mut catalog = Catalog::new();
    catalog.register("data", Batch::from_rows(
        Schema::new(vec![Field::new("x", DataType::Integer, true)]).unwrap(),
        vec![vec![Value::Null], vec![Value::Integer(7)]],
    ).unwrap()).unwrap();
    let result = execute(&parse_sql("SELECT COALESCE(x, 9223372036854775807 + 1) AS resolved FROM data WHERE x IS NOT NULL").unwrap(), &catalog).unwrap();
    assert_eq!(result.rows().unwrap(), vec![vec![Value::Integer(7)]]);
    let result = execute(&parse_sql("SELECT COALESCE(x, 5) AS resolved FROM data ORDER BY resolved").unwrap(), &catalog).unwrap();
    assert_eq!(result.rows().unwrap(), vec![vec![Value::Integer(5)], vec![Value::Integer(7)]]);
}

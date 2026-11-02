use rustspark_native::{batch::Batch, catalog::Catalog, executor::execute,
    scalar::Value, schema::{DataType, Field, Schema}, sql::parse_sql};
#[test]
fn first_value_aggregate_accepts_ignore_nulls_and_having() {
    let mut catalog = Catalog::new();
    let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
    catalog.register("t", Batch::from_rows(schema, vec![
        vec![Value::Null], vec![Value::Integer(5)], vec![Value::Integer(8)]
    ]).unwrap()).unwrap();
    let run = |sql: &str| -> rustspark_native::Result<Vec<Vec<Value>>> {
        execute(&parse_sql(sql)?, &catalog)?.rows()
    };
    assert_eq!(run("SELECT FIRST_VALUE(n) AS v FROM t").unwrap(), vec![vec![Value::Null]]);
    assert_eq!(run("SELECT FIRST_VALUE(n, TRUE) AS v FROM t").unwrap(),
        vec![vec![Value::Integer(5)]]);
    assert_eq!(run("SELECT FIRST_VALUE(n, TRUE) AS v FROM t HAVING FIRST_VALUE(n, TRUE) > 2").unwrap(),
        vec![vec![Value::Integer(5)]]);
    assert!(run("SELECT FIRST_VALUE(n, n > 0) FROM t").is_err());
}

use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn rows(query: &str) -> Vec<Vec<Value>> {
    let schema = Schema::new(vec![
        Field::new("city", DataType::Text, false),
        Field::new("revenue", DataType::Integer, true),
    ]).unwrap();
    let mut catalog = Catalog::new();
    catalog.register("sales", Batch::from_rows(schema, vec![
        vec![Value::Text("Berlin".into()), Value::Integer(3)],
        vec![Value::Text("Berlin".into()), Value::Integer(5)],
        vec![Value::Text("Paris".into()), Value::Integer(7)],
        vec![Value::Text("Paris".into()), Value::Null],
        vec![Value::Text("Rome".into()), Value::Null],
    ]).unwrap()).unwrap();
    execute(&parse_sql(query).unwrap(), &catalog).unwrap().rows().unwrap()
}

#[test]
fn spark_having_can_reference_selected_and_hidden_aggregates() {
    assert_eq!(rows(
        "SELECT city, SUM(revenue) AS total FROM sales          GROUP BY city HAVING SUM(revenue) >= 8 ORDER BY city"
    ), vec![
        vec![Value::Text("Berlin".into()), Value::Integer(8)]
    ]);
    assert_eq!(rows(
        "SELECT city, SUM(revenue) AS total FROM sales          GROUP BY city HAVING COUNT(*) >= 2 ORDER BY city"
    ), vec![
        vec![Value::Text("Berlin".into()), Value::Integer(8)],
        vec![Value::Text("Paris".into()), Value::Integer(7)],
    ]);
    assert_eq!(rows(
        "SELECT city AS place, COUNT(*) AS n FROM sales          GROUP BY city HAVING MAX(revenue) > 5 ORDER BY place"
    ), vec![vec![Value::Text("Paris".into()), Value::Integer(2)]]);
}

#[test]
fn spark_having_reuses_aliases_and_computed_grouping_keys() {
    assert_eq!(rows(
        "SELECT UPPER(city) AS region, COUNT(*) AS n FROM sales          GROUP BY UPPER(city) HAVING region = 'BERLIN' AND n > 1"
    ), vec![vec![Value::Text("BERLIN".into()), Value::Integer(2)]]);
    assert_eq!(rows(
        "SELECT COUNT(*) AS n FROM sales HAVING COUNT(*) > 4"
    ), vec![vec![Value::Integer(5)]]);
    assert_eq!(rows(
        "SELECT COUNT(*) AS n FROM sales HAVING MIN(revenue) > 9"
    ), Vec::<Vec<Value>>::new());
}

#[test]
fn having_rejects_ungrouped_columns_and_bad_aggregate_calls() {
    assert!(parse_sql("SELECT city FROM sales HAVING city = 'Berlin'").is_err());
    assert!(parse_sql("SELECT COUNT(*) AS n FROM sales HAVING revenue > 5").is_err());
    assert!(parse_sql(
        "SELECT COUNT(*) AS n FROM sales HAVING SUM(revenue, revenue) > 1"
    ).is_err());
}

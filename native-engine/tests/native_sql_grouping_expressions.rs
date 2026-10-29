use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let schema = Schema::new(vec![
        Field::new("city", DataType::Text, true),
        Field::new("amount", DataType::Integer, true),
    ]).unwrap();
    let rows = vec![
        vec![Value::Text("Berlin".into()), Value::Integer(2)],
        vec![Value::Text("berlin".into()), Value::Integer(3)],
        vec![Value::Text("Paris".into()), Value::Integer(5)],
        vec![Value::Text("Paris".into()), Value::Null],
    ];
    let mut catalog = Catalog::new();
    catalog.register("sales", Batch::from_rows(schema, rows).unwrap()).unwrap();
    catalog
}

fn rows(query: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(query).unwrap(), &catalog()).unwrap().rows().unwrap()
}

#[test]
fn spark_group_by_expressions_are_evaluated_before_aggregating() {
    assert_eq!(rows(
        "SELECT UPPER(city) AS region, SUM(amount) AS total FROM sales          GROUP BY UPPER(city) ORDER BY region"
    ), vec![
        vec![Value::Text("BERLIN".into()), Value::Integer(5)],
        vec![Value::Text("PARIS".into()), Value::Integer(5)],
    ]);
    assert_eq!(rows(
        "SELECT amount + 1 AS next_amount, COUNT(*) AS n FROM sales          GROUP BY amount + 1 ORDER BY next_amount"
    ), vec![
        vec![Value::Null, Value::Integer(1)],
        vec![Value::Integer(3), Value::Integer(1)],
        vec![Value::Integer(4), Value::Integer(1)],
        vec![Value::Integer(6), Value::Integer(1)],
    ]);
}

#[test]
fn group_by_alias_ordinal_and_all_share_the_same_results() {
    let by_expr = rows("SELECT city AS place, COUNT(*) AS n FROM sales GROUP BY city ORDER BY place");
    let by_alias = rows("SELECT city AS place, COUNT(*) AS n FROM sales GROUP BY place ORDER BY place");
    let by_ordinal = rows("SELECT city AS place, COUNT(*) AS n FROM sales GROUP BY 1 ORDER BY place");
    let by_all = rows("SELECT city AS place, COUNT(*) AS n FROM sales GROUP BY ALL ORDER BY place");
    assert_eq!(by_alias, by_expr);
    assert_eq!(by_ordinal, by_expr);
    assert_eq!(by_all, by_expr);
    assert_eq!(by_expr, vec![
        vec![Value::Text("Berlin".into()), Value::Integer(1)],
        vec![Value::Text("Paris".into()), Value::Integer(2)],
        vec![Value::Text("berlin".into()), Value::Integer(1)],
    ]);
}

#[test]
fn invalid_ordinals_and_ungrouped_expressions_fail() {
    assert!(parse_sql("SELECT city, COUNT(*) FROM sales GROUP BY 0").is_err());
    assert!(parse_sql("SELECT city, COUNT(*) FROM sales GROUP BY 3").is_err());
    assert!(parse_sql("SELECT city, COUNT(*) FROM sales GROUP BY 2").is_err());
    assert!(parse_sql("SELECT city, SUM(amount) FROM sales GROUP BY amount").is_err());
    assert!(parse_sql("SELECT city, COUNT(*) FROM sales GROUP BY city + 1").is_err());
}

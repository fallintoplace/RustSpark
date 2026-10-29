use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn data() -> Catalog {
    let schema = Schema::new(vec![
        Field::new("city", DataType::Text, false),
        Field::new("amount", DataType::Integer, true),
    ]).unwrap();
    let mut catalog = Catalog::new();
    catalog.register("sales", Batch::from_rows(schema, vec![
        vec![Value::Text("Berlin".into()), Value::Integer(3)],
        vec![Value::Text("Paris".into()), Value::Null],
        vec![Value::Text("Vienna".into()), Value::Integer(1)],
        vec![Value::Text("Rome".into()), Value::Integer(7)],
    ]).unwrap()).unwrap();
    catalog
}

fn run(query: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(query).unwrap(), &data()).unwrap().rows().unwrap()
}

#[test]
fn spark_order_by_arbitrary_computed_projected_expressions() {
    assert_eq!(run(
        "SELECT city, amount FROM sales ORDER BY amount * -1 DESC NULLS LAST"
    ), vec![
        vec![Value::Text("Vienna".into()), Value::Integer(1)],
        vec![Value::Text("Berlin".into()), Value::Integer(3)],
        vec![Value::Text("Rome".into()), Value::Integer(7)],
        vec![Value::Text("Paris".into()), Value::Null],
    ]);
    assert_eq!(run(
        "SELECT city, amount + 1 AS increased FROM sales          ORDER BY increased * -1 ASC NULLS FIRST LIMIT 3"
    ), vec![
        vec![Value::Text("Paris".into()), Value::Null],
        vec![Value::Text("Rome".into()), Value::Integer(8)],
        vec![Value::Text("Berlin".into()), Value::Integer(4)],
    ]);
}

#[test]
fn spark_order_by_functions_and_multi_column_tiebreakers() {
    assert_eq!(run(
        "SELECT city, amount FROM sales ORDER BY LENGTH(city) ASC, city DESC"
    ), vec![
        vec![Value::Text("Rome".into()), Value::Integer(7)],
        vec![Value::Text("Paris".into()), Value::Null],
        vec![Value::Text("Vienna".into()), Value::Integer(1)],
        vec![Value::Text("Berlin".into()), Value::Integer(3)],
    ]);
    assert_eq!(run(
        "SELECT city, amount FROM sales ORDER BY UPPER(city) DESC LIMIT 2"
    ), vec![
        vec![Value::Text("Vienna".into()), Value::Integer(1)],
        vec![Value::Text("Rome".into()), Value::Integer(7)],
    ]);
}

#[test]
fn ordinals_are_only_bare_positive_integers() {
    assert_eq!(run("SELECT city, amount FROM sales ORDER BY 2 DESC NULLS LAST LIMIT 1"),
        vec![vec![Value::Text("Rome".into()), Value::Integer(7)]]);
    assert_eq!(run("SELECT city FROM sales ORDER BY 1 + 1 LIMIT 2"),
        vec![vec![Value::Text("Berlin".into())], vec![Value::Text("Paris".into())]]);
    for invalid in ["SELECT city FROM sales ORDER BY 0",
                    "SELECT city FROM sales ORDER BY -1",
                    "SELECT city FROM sales ORDER BY 9",
                    "SELECT city FROM sales ORDER BY missing + 1"] {
        let query = parse_sql(invalid);
        if query.is_ok() { assert!(execute(&query.unwrap(), &data()).is_err()); }
    }
}

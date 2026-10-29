use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let mut c=Catalog::new();
    c.register("t", Batch::from_rows(
        Schema::new(vec![
            Field::new("g",DataType::Integer,false),
            Field::new("a",DataType::Integer,true),
            Field::new("b",DataType::Text,true),
        ]).unwrap(),vec![
            vec![Value::Integer(1),Value::Integer(1),Value::Text("x".into())],
            vec![Value::Integer(1),Value::Integer(1),Value::Text("x".into())],
            vec![Value::Integer(1),Value::Integer(1),Value::Text("y".into())],
            vec![Value::Integer(1),Value::Null,Value::Text("x".into())],
            vec![Value::Integer(1),Value::Integer(2),Value::Null],
            vec![Value::Integer(2),Value::Integer(2),Value::Text("x".into())],
        ],
    ).unwrap()).unwrap();
    c
}
fn rows(query:&str)->Vec<Vec<Value>>{
    execute(&parse_sql(query).unwrap(),&catalog()).unwrap().rows().unwrap()
}
#[test]
fn count_distinct_tuple_deduplicates_by_full_key_and_skips_nulls(){
    assert_eq!(rows("SELECT COUNT(DISTINCT a,b) AS n FROM t"),vec![vec![Value::Integer(3)]]);
    assert_eq!(rows("SELECT g, COUNT(DISTINCT a,b) AS n FROM t GROUP BY g ORDER BY g"),vec![
        vec![Value::Integer(1),Value::Integer(2)],
        vec![Value::Integer(2),Value::Integer(1)],
    ]);
}
#[test]
fn count_distinct_single_argument_retains_legacy_semantics(){
    assert_eq!(rows("SELECT COUNT(DISTINCT a) AS n, COUNT(DISTINCT b) AS m FROM t"),vec![
        vec![Value::Integer(2),Value::Integer(2)],
    ]);
}
#[test]
fn composite_count_distinct_filter_applies_before_deduplication(){
    assert_eq!(rows("SELECT COUNT(DISTINCT a,b) FILTER (WHERE a=1) FROM t"),
        vec![vec![Value::Integer(2)]]);
}
#[test]
fn incorrect_arg_counts_and_missing_columns_are_rejected(){
    assert!(parse_sql("SELECT COUNT(DISTINCT a,) FROM t").is_err());
    let plan=parse_sql("SELECT COUNT(DISTINCT a,missing) FROM t").unwrap();
    assert!(execute(&plan,&catalog()).is_err());
    assert!(parse_sql("SELECT COUNT(a,b) FROM t").is_err());
}

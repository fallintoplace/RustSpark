use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};
fn catalog()->Catalog{
    let mut c=Catalog::new();
    c.register("source",Batch::from_rows(Schema::new(vec![
        Field::new("id",DataType::Integer,false),
        Field::new("amount",DataType::Integer,true),
    ]).unwrap(),vec![
        vec![Value::Integer(1),Value::Integer(10)],
        vec![Value::Integer(2),Value::Integer(20)],
        vec![Value::Integer(3),Value::Integer(30)],
    ]).unwrap()).unwrap();c
}
fn rows(q:&str)->Vec<Vec<Value>>{
    execute(&parse_sql(q).unwrap(),&catalog()).unwrap().rows().unwrap()
}
#[test]
fn cte_produces_a_query_local_typed_relation(){
    assert_eq!(rows("WITH data AS (SELECT id, amount FROM source WHERE amount > 10) SELECT id FROM data ORDER BY id"),vec![
        vec![Value::Integer(2)],vec![Value::Integer(3)],
    ]);
}
#[test]
fn multiple_ctes_can_reference_earlier_ctes(){
    assert_eq!(rows("WITH base AS (SELECT id, amount FROM source), filtered AS (SELECT id FROM base WHERE amount >= 20) SELECT COUNT(*) AS n FROM filtered"),vec![vec![Value::Integer(2)]]);
}
#[test]
fn cte_column_alias_list_renames_projection_columns(){
    assert_eq!(rows("WITH typed(new_id,new_amount) AS (SELECT id, amount FROM source) SELECT new_id FROM typed ORDER BY new_id DESC LIMIT 1"),vec![
        vec![Value::Integer(3)],
    ]);
}
#[test]
fn cte_can_be_used_on_both_join_sides(){
    assert_eq!(rows("WITH a AS (SELECT id FROM source), b AS (SELECT id FROM source WHERE id > 1) SELECT id FROM a LEFT SEMI JOIN b ON id = id ORDER BY id"),vec![
        vec![Value::Integer(2)],vec![Value::Integer(3)],
    ]);
}
#[test]
fn duplicate_names_and_recursion_fail_closed(){
    assert!(parse_sql("WITH x AS (SELECT id FROM source), X AS (SELECT id FROM source) SELECT id FROM x").is_err());
    assert!(parse_sql("WITH RECURSIVE x AS (SELECT id FROM source) SELECT * FROM x").is_err());
    assert!(parse_sql("WITH x AS (SELECT id FROM x) SELECT id FROM x").is_err());
    assert!(parse_sql("WITH x(a) AS (SELECT id,amount FROM source) SELECT a FROM x").is_err());
}
#[test]
fn cte_bindings_do_not_leak_across_parses(){
    assert_eq!(rows("WITH x AS (SELECT id FROM source) SELECT id FROM x").len(),3);
    let other=parse_sql("SELECT id FROM x").unwrap();
    assert!(execute(&other,&catalog()).is_err());
}

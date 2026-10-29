use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};
fn catalog()->Catalog{
    let mut c=Catalog::new();
    c.register("source",Batch::from_rows(Schema::new(vec![
        Field::new("id",DataType::Integer,false),
        Field::new("amount",DataType::Integer,false),
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
fn from_subquery_filters_and_projects_without_global_catalog_changes(){
    assert_eq!(rows("SELECT id FROM (SELECT id, amount FROM source WHERE amount > 10) AS derived ORDER BY id"),vec![
        vec![Value::Integer(2)],vec![Value::Integer(3)],
    ]);
}
#[test]
fn nested_derived_tables_can_reuse_column_aliases(){
    assert_eq!(rows("SELECT key FROM (SELECT renamed FROM (SELECT id AS renamed FROM source) AS x) AS y(key) ORDER BY key DESC LIMIT 1"),vec![
        vec![Value::Integer(3)],
    ]);
}
#[test]
fn joins_can_reference_two_independent_derived_relations(){
    assert_eq!(rows("SELECT amount FROM (SELECT id, amount FROM source) AS a JOIN (SELECT id FROM source WHERE id > 1) AS b ON id = id ORDER BY amount"),vec![
        vec![Value::Integer(20)],vec![Value::Integer(30)],
    ]);
}
#[test]
fn ctes_and_derived_tables_compose_without_name_leakage(){
    assert_eq!(rows("WITH base AS (SELECT id,amount FROM source) SELECT id FROM (SELECT id FROM base WHERE amount < 30) AS filtered ORDER BY id"),vec![
        vec![Value::Integer(1)],vec![Value::Integer(2)],
    ]);
}
#[test]
fn nested_order_by_limit_remains_inside_derived_query(){
    assert_eq!(rows("SELECT id FROM (SELECT id FROM source ORDER BY id DESC LIMIT 2) AS latest ORDER BY id"),vec![
        vec![Value::Integer(2)],vec![Value::Integer(3)],
    ]);
}
#[test]
fn invalid_derived_alias_counts_and_missing_parentheses_fail(){
    for query in [
        "SELECT id FROM (SELECT id FROM source) AS d(a,b)",
        "SELECT id FROM (SELECT id FROM source AS d",
        "SELECT id FROM () AS d",
    ] {
        assert!(parse_sql(query).is_err(),"{query}");
    }
}

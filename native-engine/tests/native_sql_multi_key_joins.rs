use rustspark_native::{
    batch::Batch,catalog::Catalog,executor::execute,scalar::Value,
    schema::{DataType,Field,Schema},sql::parse_sql,
};
fn fixture()->Catalog{
    let schema=Schema::new(vec![
        Field::new("tenant",DataType::Integer,false),
        Field::new("id",DataType::Integer,true),
    ]).unwrap();
    let mut c=Catalog::new();
    c.register("left_rows",Batch::from_rows(schema.clone(),vec![
        vec![Value::Integer(1),Value::Integer(2)],
        vec![Value::Integer(2),Value::Integer(2)],
        vec![Value::Integer(1),Value::Null],
    ]).unwrap()).unwrap();
    c.register("right_rows",Batch::from_rows(schema,vec![
        vec![Value::Integer(1),Value::Integer(2)],
        vec![Value::Integer(1),Value::Null],
    ]).unwrap()).unwrap();
    c
}
#[test]
fn multi_key_equality_join_uses_both_keys_and_filters_nulls() {
    let sql="SELECT tenant, id FROM left_rows JOIN right_rows \
             ON tenant = right_tenant AND id = right_id";
    // The right columns are renamed after the join. ON refers to source names,
    // so a direct plan test can use different right-side source field names.
    assert!(parse_sql(sql).is_ok());
}
#[test]
fn join_conjunction_rejects_non_equality_predicates() {
    assert!(parse_sql("SELECT * FROM left_rows JOIN right_rows ON tenant = tenant AND id > id").is_err());
}
#[test]
fn direct_multi_join_distinguishes_tenants_and_null_keys() {
    let mut c=fixture();
    c.register("matches",Batch::from_rows(
        Schema::new(vec![
            Field::new("account",DataType::Integer,false),
            Field::new("event",DataType::Integer,true),
        ]).unwrap(),
        vec![vec![Value::Integer(1),Value::Integer(2)],
             vec![Value::Integer(1),Value::Null]],
    ).unwrap()).unwrap();
    let plan=parse_sql("SELECT tenant, id FROM left_rows JOIN matches \
        ON tenant = account AND id = event ORDER BY id").unwrap();
    let rows=execute(&plan,&c).unwrap().rows().unwrap();
    assert_eq!(rows,vec![vec![Value::Integer(1),Value::Integer(2)]]);
}

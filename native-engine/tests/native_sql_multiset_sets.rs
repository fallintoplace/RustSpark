use rustspark_native::{batch::Batch,catalog::Catalog,executor::execute,scalar::Value,schema::{DataType,Field,Schema},sql::parse_sql};
fn catalog() -> Catalog {
    let schema = Schema::new(vec![Field::new("id",DataType::Integer,true)]).unwrap();
    let mut c = Catalog::new();
    c.register("left_rows",Batch::from_rows(schema.clone(),vec![
        vec![Value::Integer(1)],vec![Value::Integer(1)],vec![Value::Integer(1)],
        vec![Value::Integer(2)],vec![Value::Null],vec![Value::Null],
    ]).unwrap()).unwrap();
    c.register("right_rows",Batch::from_rows(schema,vec![
        vec![Value::Integer(1)],vec![Value::Integer(1)],
        vec![Value::Integer(3)],vec![Value::Null],
    ]).unwrap()).unwrap();
    c
}
fn rows(sql: &str)->Vec<Vec<Value>>{
    execute(&parse_sql(sql).unwrap(),&catalog()).unwrap().rows().unwrap()
}
#[test]
fn intersect_all_preserves_minimum_multiplicity_including_nulls() {
    assert_eq!(rows("SELECT id FROM left_rows INTERSECT ALL SELECT id FROM right_rows"),vec![
        vec![Value::Integer(1)],vec![Value::Integer(1)],vec![Value::Null]
    ]);
    assert_eq!(rows("SELECT id FROM left_rows INTERSECT DISTINCT SELECT id FROM right_rows").len(),2);
}
#[test]
fn except_all_subtracts_multiplicity_without_discarding_duplicates() {
    assert_eq!(rows("SELECT id FROM left_rows EXCEPT ALL SELECT id FROM right_rows"),vec![
        vec![Value::Integer(1)],vec![Value::Integer(2)],vec![Value::Null]
    ]);
    assert_eq!(rows("SELECT id FROM left_rows EXCEPT SELECT id FROM right_rows"),vec![
        vec![Value::Integer(2)]
    ]);
}

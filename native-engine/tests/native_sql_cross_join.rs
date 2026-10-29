use rustspark_native::{
    batch::Batch,catalog::Catalog,executor::execute,scalar::Value,
    schema::{DataType,Field,Schema},sql::parse_sql,
};
fn catalog()->Catalog{
    let mut c=Catalog::new();
    c.register("left_rows",Batch::from_rows(
        Schema::new(vec![Field::new("id",DataType::Integer,false)]).unwrap(),
        vec![vec![Value::Integer(1)],vec![Value::Integer(2)]],
    ).unwrap()).unwrap();
    c.register("right_rows",Batch::from_rows(
        Schema::new(vec![Field::new("id",DataType::Integer,false)]).unwrap(),
        vec![vec![Value::Integer(3)],vec![Value::Integer(4)]],
    ).unwrap()).unwrap();
    c
}
#[test]
fn cartesian_product_generates_all_combinations_without_equality_key() {
    let sql="SELECT id, right_id FROM left_rows CROSS JOIN right_rows WHERE id = 2 ORDER BY right_id";
    let batch=execute(&parse_sql(sql).unwrap(),&catalog()).unwrap();
    assert_eq!(batch.rows().unwrap(),vec![
        vec![Value::Integer(2),Value::Integer(3)],
        vec![Value::Integer(2),Value::Integer(4)],
    ]);
    assert_eq!(execute(&parse_sql("SELECT * FROM left_rows CROSS JOIN right_rows").unwrap(),&catalog()).unwrap().num_rows(),4);
}
#[test]
fn cross_join_empty_inputs_produce_schema_and_zero_rows() {
    let mut c=catalog();
    let schema=Schema::new(vec![Field::new("x",DataType::Integer,false)]).unwrap();
    c.register("empty",Batch::empty(schema)).unwrap();
    let result=execute(&parse_sql("SELECT * FROM left_rows CROSS JOIN empty").unwrap(),&c).unwrap();
    assert_eq!(result.num_rows(),0);
    assert_eq!(result.schema().len(),2);
}
#[test]
fn cross_join_rejects_unbounded_cartesian_explosion() {
    let schema=Schema::new(vec![Field::new("x",DataType::Integer,false)]).unwrap();
    let side=Batch::from_rows(schema,(0..1001).map(|n|vec![Value::Integer(n)]).collect()).unwrap();
    assert!(rustspark_native::join::cross_join(&side,&side).is_err());
}

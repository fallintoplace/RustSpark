use rustspark_native::{
    batch::Batch,join::{hash_join,JoinKind},scalar::Value,schema::{DataType,Field,Schema},
};
fn input(kind:DataType,values:Vec<Value>)->Batch{
    Batch::from_rows(Schema::new(vec![Field::new("key",kind,true)]).unwrap(),
        values.into_iter().map(|v|vec![v]).collect()).unwrap()
}
#[test]
fn mixed_numeric_join_types_are_compared_after_widening() {
    let ints=input(DataType::Integer,vec![Value::Integer(1),Value::Integer(2),
        Value::Integer(3),Value::Null]);
    let doubles=input(DataType::Float,vec![Value::Float(2.0),Value::Float(3.5),Value::Null]);
    let inner=hash_join(&ints,&doubles,"key","key",JoinKind::Inner).unwrap();
    assert_eq!(inner.rows().unwrap(),vec![vec![Value::Integer(2),Value::Float(2.0)]]);
    assert_eq!(hash_join(&ints,&doubles,"key","key",JoinKind::Semi).unwrap().num_rows(),1);
    assert_eq!(hash_join(&ints,&doubles,"key","key",JoinKind::Anti).unwrap().num_rows(),3);
    let reverse=hash_join(&doubles,&ints,"key","key",JoinKind::Left).unwrap();
    assert_eq!(reverse.num_rows(),3);
    assert_eq!(reverse.schema().field(0).unwrap().data_type,DataType::Float);
    assert_eq!(reverse.schema().field(1).unwrap().data_type,DataType::Integer);
}
#[test]
fn mixed_numeric_joins_reject_unrelated_sql_types() {
    let ints=input(DataType::Integer,vec![Value::Integer(2)]);
    let texts=input(DataType::Text,vec![Value::Text("2".into())]);
    assert!(hash_join(&ints,&texts,"key","key",JoinKind::Inner).is_err());
}

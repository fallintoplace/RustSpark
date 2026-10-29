use rustspark_native::{
    batch::Batch, scalar::Value, schema::{DataType,Field,Schema}, set_ops,
};

fn input(kind:DataType,values:Vec<Value>)->Batch{
    Batch::from_rows(Schema::new(vec![Field::new("value",kind,true)]).unwrap(),
        values.into_iter().map(|v|vec![v]).collect()).unwrap()
}
#[test]
fn set_distinct_uses_promoted_numeric_equality() {
    let a=input(DataType::Integer,vec![Value::Integer(2),Value::Integer(2),
        Value::Integer(3),Value::Null]);
    let b=input(DataType::Float,vec![Value::Float(2.0),Value::Float(2.0),
        Value::Float(4.0),Value::Null]);
    let intersect=set_ops::intersect_distinct(&a,&b).unwrap();
    assert_eq!(intersect.rows().unwrap(),vec![vec![Value::Float(2.0)],vec![Value::Null]]);
    assert_eq!(intersect.schema().field(0).unwrap().data_type,DataType::Float);
    assert_eq!(set_ops::except_distinct(&a,&b).unwrap().rows().unwrap(),
        vec![vec![Value::Float(3.0)]]);
}
#[test]
fn set_all_matches_multiplicities_after_numeric_coercion() {
    let a=input(DataType::Integer,vec![Value::Integer(2),Value::Integer(2),Value::Integer(2),
        Value::Null,Value::Null]);
    let b=input(DataType::Float,vec![Value::Float(2.0),Value::Float(2.0),Value::Null]);
    assert_eq!(set_ops::intersect_all(&a,&b).unwrap().rows().unwrap(),vec![
        vec![Value::Float(2.0)],vec![Value::Float(2.0)],vec![Value::Null]]);
    assert_eq!(set_ops::except_all(&a,&b).unwrap().rows().unwrap(),vec![
        vec![Value::Float(2.0)],vec![Value::Null]]);
}
#[test]
fn incompatible_types_remain_errors() {
    let a=input(DataType::Integer,vec![Value::Integer(1)]);
    let b=input(DataType::Text,vec![Value::Text("1".into())]);
    assert!(set_ops::intersect_all(&a,&b).is_err());
    assert!(set_ops::except_distinct(&a,&b).is_err());
}

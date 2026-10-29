use rustspark_native::{
    aggregate::{aggregate,AggregateItem,AggregateOp},batch::Batch,expr::{BinaryOp,Expr},
    join::{hash_join,JoinKind},scalar::Value,schema::{DataType,Field,Schema},
};
fn input(values:Vec<Value>)->Batch{
    Batch::from_rows(Schema::new(vec![Field::new("n",DataType::Float,true)]).unwrap(),
        values.into_iter().map(|value|vec![value]).collect()).unwrap()
}
#[test]
fn spark_nan_predicates_are_total_and_distinct_from_nulls() {
    let nan=Value::Float(f64::NAN);
    assert_eq!(nan.sql_equal(&nan).unwrap(),Some(true));
    assert_eq!(nan.sql_equal(&Value::Float(0.0)).unwrap(),Some(false));
    assert!(nan.sql_compare(&Value::Float(f64::INFINITY)).unwrap().unwrap().is_gt());
    assert!(Value::Integer(100).sql_compare(&nan).unwrap().unwrap().is_lt());
    assert_eq!(nan.sql_equal(&Value::Null).unwrap(),None);
}
#[test]
fn nan_join_keys_match_and_group_into_single_bucket() {
    let left=input(vec![Value::Float(f64::NAN),Value::Float(f64::NAN),Value::Float(1.0),Value::Null]);
    let right=input(vec![Value::Float(f64::NAN),Value::Float(1.0)]);
    assert_eq!(hash_join(&left,&right,"n","n",JoinKind::Inner).unwrap().num_rows(),3);
    assert_eq!(hash_join(&left,&right,"n","n",JoinKind::Anti).unwrap().num_rows(),1);
    let grouped=aggregate(&left,&[(Expr::Column("n".into()),"n".into())],
        &[AggregateItem::new(AggregateOp::CountAll,"total")]).unwrap();
    assert_eq!(grouped.num_rows(),3);
    let nan_row=grouped.rows().unwrap().into_iter()
        .find(|row|matches!(&row[0],Value::Float(x) if x.is_nan())).unwrap();
    assert_eq!(nan_row[1],Value::Integer(2));
}
#[test]
fn nan_relational_operators_match_spark_results() {
    let b=input(vec![Value::Float(f64::NAN)]);
    let expression=Expr::binary(Expr::Column("n".into()),BinaryOp::Greater,
        Expr::Literal(Value::Float(5.0)));
    assert_eq!(expression.evaluate(&b,0).unwrap(),Value::Boolean(true));
    let expression=Expr::binary(Expr::Column("n".into()),BinaryOp::Equal,
        Expr::Literal(Value::Float(f64::NAN)));
    assert_eq!(expression.evaluate(&b,0).unwrap(),Value::Boolean(true));
}

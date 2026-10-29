use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType,Field,Schema}, sql::parse_sql, union,
};

fn batch(type_: DataType, nullable: bool, values: &[Value]) -> Batch {
    Batch::from_rows(Schema::new(vec![Field::new("number",type_,nullable)]).unwrap(),
        values.iter().cloned().map(|value| vec![value]).collect()).unwrap()
}
#[test]
fn union_promotes_integer_and_float_to_common_numeric_type() {
    let left = batch(DataType::Integer,false,&[Value::Integer(1),Value::Integer(2)]);
    let right = batch(DataType::Float,true,&[Value::Float(1.0),Value::Null]);
    let merged = union::union_all(&left,&right).unwrap();
    assert_eq!(merged.schema().field(0).unwrap().data_type,DataType::Float);
    assert!(merged.schema().field(0).unwrap().nullable);
    assert_eq!(merged.rows().unwrap(),vec![
        vec![Value::Float(1.0)],vec![Value::Float(2.0)],
        vec![Value::Float(1.0)],vec![Value::Null],
    ]);
    assert_eq!(union::union_distinct(&left,&right).unwrap().num_rows(),3);
}
#[test]
fn sql_union_all_coerces_mixed_numeric_tables() {
    let mut c=Catalog::new();
    c.register("ints",batch(DataType::Integer,false,&[Value::Integer(2)])).unwrap();
    c.register("floats",batch(DataType::Float,false,&[Value::Float(2.5)])).unwrap();
    let sql="SELECT number FROM ints UNION ALL SELECT number FROM floats";
    let result=execute(&parse_sql(sql).unwrap(),&c).unwrap();
    assert_eq!(result.rows().unwrap(),vec![vec![Value::Float(2.0)],vec![Value::Float(2.5)]]);
}
#[test]
fn incompatible_types_still_fail_and_no_implicit_text_coercion_occurs() {
    let ints=batch(DataType::Integer,false,&[Value::Integer(1)]);
    let text=batch(DataType::Text,false,&[Value::Text("1".into())]);
    assert!(union::union_all(&ints,&text).is_err());
}

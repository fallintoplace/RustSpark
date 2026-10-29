use rustspark_native::{batch::Batch,catalog::Catalog,executor::execute,scalar::Value,schema::{DataType,Field,Schema},sql::parse_sql};
#[test]
fn except_returns_distinct_left_rows_not_present_on_right() {
 let schema=Schema::new(vec![Field::new("id",DataType::Integer,true)]).unwrap();
 let mut catalog=Catalog::new();
 catalog.register("a",Batch::from_rows(schema.clone(),vec![vec![Value::Integer(1)],vec![Value::Integer(2)],vec![Value::Integer(2)],vec![Value::Null]]).unwrap()).unwrap();
 catalog.register("b",Batch::from_rows(schema,vec![vec![Value::Integer(1)],vec![Value::Integer(3)],vec![Value::Null]]).unwrap()).unwrap();
 let result=execute(&parse_sql("SELECT * FROM a EXCEPT SELECT * FROM b ORDER BY id").unwrap(),&catalog).unwrap();
 assert_eq!(result.rows().unwrap(),vec![vec![Value::Integer(2)]]);
}

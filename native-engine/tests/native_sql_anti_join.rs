use rustspark_native::{batch::Batch,catalog::Catalog,executor::execute,scalar::Value,schema::{DataType,Field,Schema},sql::parse_sql};
#[test]
fn anti_join_keeps_null_and_unmatched_left_keys() {
 let schema=|name| Schema::new(vec![Field::new(name,DataType::Integer,true)]).unwrap();
 let mut catalog=Catalog::new();
 catalog.register("l",Batch::from_rows(schema("a"),vec![vec![Value::Integer(1)],vec![Value::Integer(2)],vec![Value::Null]]).unwrap()).unwrap();
 catalog.register("r",Batch::from_rows(schema("b"),vec![vec![Value::Integer(1)],vec![Value::Null]]).unwrap()).unwrap();
 let result=execute(&parse_sql("SELECT * FROM l LEFT ANTI JOIN r ON a = b").unwrap(),&catalog).unwrap();
 assert_eq!(result.rows().unwrap(),vec![vec![Value::Integer(2)],vec![Value::Null]]);
 assert_eq!(result.schema().len(),1);
}

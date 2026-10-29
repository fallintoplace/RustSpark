use rustspark_native::{batch::Batch,catalog::Catalog,executor::execute,scalar::Value,schema::{DataType,Field,Schema},sql::parse_sql};
#[test]
fn full_join_preserves_both_unmatched_sides() {
 let schema=|name| Schema::new(vec![Field::new(name,DataType::Integer,true)]).unwrap();
 let mut catalog=Catalog::new();
 catalog.register("l",Batch::from_rows(schema("a"),vec![vec![Value::Integer(1)],vec![Value::Integer(3)],vec![Value::Null]]).unwrap()).unwrap();
 catalog.register("r",Batch::from_rows(schema("b"),vec![vec![Value::Integer(1)],vec![Value::Integer(2)],vec![Value::Null]]).unwrap()).unwrap();
 let result=execute(&parse_sql("SELECT * FROM l FULL OUTER JOIN r ON a = b").unwrap(),&catalog).unwrap();
 assert_eq!(result.rows().unwrap(),vec![
 vec![Value::Integer(1),Value::Integer(1)],
 vec![Value::Integer(3),Value::Null],
 vec![Value::Null,Value::Null],
 vec![Value::Null,Value::Integer(2)],
 vec![Value::Null,Value::Null],
 ]);
 assert!(result.schema().fields().iter().all(|f|f.nullable));
}

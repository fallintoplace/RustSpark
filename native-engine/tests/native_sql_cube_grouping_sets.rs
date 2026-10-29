use rustspark_native::{
    batch::Batch,catalog::Catalog,executor::execute,scalar::Value,
    schema::{DataType,Field,Schema},sql::parse_sql,
};
fn catalog()->Catalog{
    let mut c=Catalog::new();
    c.register("sales",Batch::from_rows(Schema::new(vec![
        Field::new("city",DataType::Text,true),
        Field::new("item",DataType::Text,true),
        Field::new("qty",DataType::Integer,false),
    ]).unwrap(),vec![
        vec![Value::Text("A".into()),Value::Text("x".into()),Value::Integer(2)],
        vec![Value::Text("A".into()),Value::Text("y".into()),Value::Integer(3)],
        vec![Value::Text("B".into()),Value::Text("x".into()),Value::Integer(5)],
    ]).unwrap()).unwrap();c
}
fn rows(q:&str)->Vec<Vec<Value>>{
    execute(&parse_sql(q).unwrap(),&catalog()).unwrap().rows().unwrap()
}
#[test]
fn cube_builds_subtotals_across_each_dimension(){
    let got=rows("SELECT city,item,SUM(qty) AS total FROM sales GROUP BY CUBE(city,item)");
    assert_eq!(got.len(),8);
    for row in [
        vec![Value::Text("A".into()),Value::Null,Value::Integer(5)],
        vec![Value::Text("B".into()),Value::Null,Value::Integer(5)],
        vec![Value::Null,Value::Text("x".into()),Value::Integer(7)],
        vec![Value::Null,Value::Text("y".into()),Value::Integer(3)],
        vec![Value::Null,Value::Null,Value::Integer(10)],
    ] {assert!(got.contains(&row),"{row:?}");}
}
#[test]
fn explicit_grouping_sets_keep_only_requested_dimensions(){
    let got=rows("SELECT city,item,SUM(qty) FROM sales GROUP BY GROUPING SETS ((city,item),(item),())");
    assert_eq!(got.len(),6);
    assert!(got.contains(&vec![Value::Null,Value::Text("x".into()),Value::Integer(7)]));
    assert!(!got.contains(&vec![Value::Text("A".into()),Value::Null,Value::Integer(5)]));
}
#[test]
fn repeated_grouping_sets_duplicate_rows_instead_of_deduplicating(){
    let got=rows("SELECT SUM(qty) AS total FROM sales GROUP BY GROUPING SETS ((),())");
    assert_eq!(got,vec![
        vec![Value::Integer(10)],vec![Value::Integer(10)],
    ]);
}
#[test]
fn single_expression_grouping_set_is_supported(){
    let got=rows("SELECT city,COUNT(*) FROM sales GROUP BY GROUPING SETS (city,())");
    assert_eq!(got.len(),3);
    assert!(got.contains(&vec![Value::Null,Value::Integer(3)]));
}
#[test]
fn cube_empty_source_retains_one_global_total(){
    assert_eq!(rows("SELECT city,COUNT(*) FROM sales WHERE FALSE GROUP BY CUBE(city)"),
        vec![vec![Value::Null,Value::Integer(0)]]);
}
#[test]
fn invalid_grouping_sets_do_not_trigger_unbounded_expansion(){
    assert!(parse_sql("SELECT SUM(qty) FROM sales GROUP BY GROUPING SETS ()").is_err());
    assert!(parse_sql("SELECT SUM(qty) FROM sales GROUP BY GROUPING SETS ((city,city))").is_err());
    let many=std::iter::repeat_n("city",9).collect::<Vec<_>>().join(",");
    assert!(parse_sql(&format!("SELECT COUNT(*) FROM sales GROUP BY CUBE({many})")).is_err());
}

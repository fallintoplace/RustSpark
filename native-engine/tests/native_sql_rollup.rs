use rustspark_native::{
    batch::Batch,catalog::Catalog,executor::execute,scalar::Value,
    schema::{DataType,Field,Schema},sql::parse_sql,
};
fn catalog()->Catalog {
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
fn rollup_emits_detail_subtotals_and_grand_total(){
    let got=rows("SELECT city,item,SUM(qty) AS total FROM sales GROUP BY ROLLUP(city,item)");
    assert_eq!(got.len(),6);
    for wanted in [
        vec![Value::Text("A".into()),Value::Text("x".into()),Value::Integer(2)],
        vec![Value::Text("A".into()),Value::Text("y".into()),Value::Integer(3)],
        vec![Value::Text("B".into()),Value::Text("x".into()),Value::Integer(5)],
        vec![Value::Text("A".into()),Value::Null,Value::Integer(5)],
        vec![Value::Text("B".into()),Value::Null,Value::Integer(5)],
        vec![Value::Null,Value::Null,Value::Integer(10)],
    ] { assert!(got.contains(&wanted),"{wanted:?}"); }
}
#[test]
fn rollup_one_dimension_has_detail_groups_and_a_total(){
    let got=rows("SELECT city,COUNT(*) AS count FROM sales GROUP BY ROLLUP(city)");
    assert_eq!(got.len(),3);
    assert!(got.contains(&vec![Value::Null,Value::Integer(3)]));
}
#[test]
fn rollup_empty_source_retains_the_grand_total(){
    assert_eq!(rows("SELECT city,COUNT(*) AS n FROM sales WHERE FALSE GROUP BY ROLLUP(city)"),
        vec![vec![Value::Null,Value::Integer(0)]]);
}
#[test]
fn rollup_keeps_duplicate_null_values_when_grouping_sets_overlap(){
    let mut catalog=catalog();
    let schema=catalog.get("sales").unwrap().schema().clone();
    catalog.register("nullable",Batch::from_rows(schema,vec![
        vec![Value::Text("A".into()),Value::Null,Value::Integer(2)],
    ]).unwrap()).unwrap();
    let rows=execute(&parse_sql("SELECT city,item,SUM(qty) FROM nullable GROUP BY ROLLUP(city,item)").unwrap(),&catalog).unwrap().rows().unwrap();
    assert_eq!(rows.len(),3);
    assert_eq!(rows.iter().filter(|r|r[0]==Value::Text("A".into())&&r[1]==Value::Null).count(),2);
}
#[test]
fn rollup_rejects_duplicate_and_unbounded_dimensions(){
    assert!(parse_sql("SELECT city,SUM(qty) FROM sales GROUP BY ROLLUP(city,city)").is_err());
    let columns=std::iter::repeat_n("city",9).collect::<Vec<_>>().join(",");
    assert!(parse_sql(&format!("SELECT COUNT(*) FROM sales GROUP BY ROLLUP({columns})")).is_err());
}

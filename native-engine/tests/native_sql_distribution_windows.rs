use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};
fn catalog()->Catalog{
    let mut c=Catalog::new();
    c.register("t",Batch::from_rows(Schema::new(vec![
        Field::new("id",DataType::Integer,false),
        Field::new("grp",DataType::Text,false),
        Field::new("score",DataType::Integer,false),
    ]).unwrap(),vec![
        vec![Value::Integer(1),Value::Text("A".into()),Value::Integer(1)],
        vec![Value::Integer(2),Value::Text("A".into()),Value::Integer(1)],
        vec![Value::Integer(3),Value::Text("A".into()),Value::Integer(2)],
        vec![Value::Integer(4),Value::Text("A".into()),Value::Integer(3)],
        vec![Value::Integer(5),Value::Text("B".into()),Value::Integer(4)],
        vec![Value::Integer(6),Value::Text("B".into()),Value::Integer(4)],
        vec![Value::Integer(7),Value::Text("C".into()),Value::Integer(8)],
    ]).unwrap()).unwrap();c
}
fn rows(q:&str)->Vec<Vec<Value>>{
    execute(&parse_sql(q).unwrap(),&catalog()).unwrap().rows().unwrap()
}
#[test]
fn percent_rank_uses_rank_not_dense_rank_and_returns_zero_for_singletons(){
    let got=rows("SELECT id, PERCENT_RANK() OVER (PARTITION BY grp ORDER BY score) AS pct FROM t ORDER BY id");
    assert_eq!(got,vec![
        vec![Value::Integer(1),Value::Float(0.0)],
        vec![Value::Integer(2),Value::Float(0.0)],
        vec![Value::Integer(3),Value::Float(2.0/3.0)],
        vec![Value::Integer(4),Value::Float(1.0)],
        vec![Value::Integer(5),Value::Float(0.0)],
        vec![Value::Integer(6),Value::Float(0.0)],
        vec![Value::Integer(7),Value::Float(0.0)],
    ]);
}
#[test]
fn cume_dist_includes_all_tied_rows_in_the_same_fraction(){
    let got=rows("SELECT id, CUME_DIST() OVER (PARTITION BY grp ORDER BY score) AS cd FROM t ORDER BY id");
    assert_eq!(got,vec![
        vec![Value::Integer(1),Value::Float(0.5)],
        vec![Value::Integer(2),Value::Float(0.5)],
        vec![Value::Integer(3),Value::Float(0.75)],
        vec![Value::Integer(4),Value::Float(1.0)],
        vec![Value::Integer(5),Value::Float(1.0)],
        vec![Value::Integer(6),Value::Float(1.0)],
        vec![Value::Integer(7),Value::Float(1.0)],
    ]);
}
#[test]
fn distribution_window_schemas_are_float_and_required(){
    let batch=execute(&parse_sql("SELECT CUME_DIST() OVER (ORDER BY score) AS cd FROM t").unwrap(),&catalog()).unwrap();
    assert_eq!(batch.schema().fields()[0].data_type,DataType::Float);
    assert!(!batch.schema().fields()[0].nullable);
}
#[test]
fn rankings_remain_consistent_with_percent_rank_and_cume_dist(){
    let got=rows("SELECT RANK() OVER (PARTITION BY grp ORDER BY score) AS r, PERCENT_RANK() OVER (PARTITION BY grp ORDER BY score) AS p FROM t");
    assert_eq!(got[2],vec![Value::Integer(3),Value::Float(2.0/3.0)]);
}
#[test]
fn malformed_distribution_calls_are_rejected(){
    assert!(parse_sql("SELECT CUME_DIST(1) OVER (ORDER BY score) FROM t").is_err());
    assert!(parse_sql("SELECT PERCENT_RANK() OVER () FROM t").is_err());
}

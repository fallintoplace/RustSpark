use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};
fn catalog() -> Catalog {
    let mut c=Catalog::new();
    c.register("t",Batch::from_rows(
        Schema::new(vec![
            Field::new("id",DataType::Integer,false),
            Field::new("dept",DataType::Text,false),
            Field::new("score",DataType::Integer,true),
        ]).unwrap(),vec![
            vec![Value::Integer(1),Value::Text("A".into()),Value::Integer(10)],
            vec![Value::Integer(2),Value::Text("A".into()),Value::Integer(20)],
            vec![Value::Integer(3),Value::Text("A".into()),Value::Null],
            vec![Value::Integer(4),Value::Text("B".into()),Value::Integer(7)],
            vec![Value::Integer(5),Value::Text("B".into()),Value::Integer(9)],
        ],
    ).unwrap()).unwrap();c
}
fn rows(query:&str)->Vec<Vec<Value>>{
    execute(&parse_sql(query).unwrap(),&catalog()).unwrap().rows().unwrap()
}
#[test]
fn lag_and_lead_offset_default_handle_partitions_and_nulls(){
    assert_eq!(rows("SELECT id, LAG(score, 1, -1) OVER (PARTITION BY dept ORDER BY id) AS prev, LEAD(score, 1, -1) OVER (PARTITION BY dept ORDER BY id) AS next FROM t ORDER BY id"),vec![
        vec![Value::Integer(1),Value::Integer(-1),Value::Integer(20)],
        vec![Value::Integer(2),Value::Integer(10),Value::Null],
        vec![Value::Integer(3),Value::Integer(20),Value::Integer(-1)],
        vec![Value::Integer(4),Value::Integer(-1),Value::Integer(9)],
        vec![Value::Integer(5),Value::Integer(7),Value::Integer(-1)],
    ]);
}
#[test]
fn zero_offset_and_default_null_keep_original_value_type(){
    assert_eq!(rows("SELECT id, LAG(score, 0) OVER (ORDER BY id) AS lagged FROM t ORDER BY id"),vec![
        vec![Value::Integer(1),Value::Integer(10)],
        vec![Value::Integer(2),Value::Integer(20)],
        vec![Value::Integer(3),Value::Null],
        vec![Value::Integer(4),Value::Integer(7)],
        vec![Value::Integer(5),Value::Integer(9)],
    ]);
    let data=execute(&parse_sql("SELECT LEAD(score) OVER (ORDER BY id) AS upcoming FROM t").unwrap(),&catalog()).unwrap();
    assert!(data.schema().fields()[0].nullable);
}
#[test]
fn default_is_only_used_when_target_row_falls_outside_partition(){
    assert_eq!(rows("SELECT id, LEAD(score, 2, 99) OVER (PARTITION BY dept ORDER BY id) AS after_two FROM t ORDER BY id"),vec![
        vec![Value::Integer(1),Value::Null],
        vec![Value::Integer(2),Value::Integer(99)],
        vec![Value::Integer(3),Value::Integer(99)],
        vec![Value::Integer(4),Value::Integer(99)],
        vec![Value::Integer(5),Value::Integer(99)],
    ]);
}
#[test]
fn unsupported_offsets_and_type_mismatches_fail(){
    assert!(parse_sql("SELECT LAG(score,-1) OVER (ORDER BY id) FROM t").is_err());
    assert!(parse_sql("SELECT LEAD(score,1000001) OVER (ORDER BY id) FROM t").is_err());
    assert!(parse_sql("SELECT LEAD(score,1,) OVER (ORDER BY id) FROM t").is_err());
    let mismatch=parse_sql("SELECT LAG(score,1,'not numeric') OVER (ORDER BY id) FROM t").unwrap();
    assert!(execute(&mismatch,&catalog()).is_err());
}
#[test]
fn ranking_functions_still_work_after_adding_navigation(){
    assert_eq!(rows("SELECT id, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY id) AS rn FROM t ORDER BY id")[3],
        vec![Value::Integer(4),Value::Integer(1)]);
}

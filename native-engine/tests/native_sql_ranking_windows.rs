use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
    stages::{StageGraph,StageKind},
};

fn catalog() -> Catalog {
    let mut c=Catalog::new();
    c.register("t",Batch::from_rows(
        Schema::new(vec![
            Field::new("id",DataType::Integer,false),
            Field::new("dept",DataType::Text,false),
            Field::new("score",DataType::Integer,true),
        ]).unwrap(),vec![
            vec![Value::Integer(1),Value::Text("A".into()),Value::Integer(2)],
            vec![Value::Integer(2),Value::Text("A".into()),Value::Integer(2)],
            vec![Value::Integer(3),Value::Text("A".into()),Value::Integer(5)],
            vec![Value::Integer(4),Value::Text("A".into()),Value::Null],
            vec![Value::Integer(5),Value::Text("B".into()),Value::Integer(1)],
            vec![Value::Integer(6),Value::Text("B".into()),Value::Integer(1)],
            vec![Value::Integer(7),Value::Text("B".into()),Value::Integer(3)],
        ],
    ).unwrap()).unwrap();
    c
}
fn rows(query:&str)->Vec<Vec<Value>>{
    execute(&parse_sql(query).unwrap(),&catalog()).unwrap().rows().unwrap()
}

#[test]
fn row_number_rank_and_dense_rank_respect_partition_order_and_ties(){
    let got=rows(
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) AS rn, RANK() OVER (PARTITION BY dept ORDER BY score DESC) AS r, DENSE_RANK() OVER (PARTITION BY dept ORDER BY score DESC) AS dr FROM t ORDER BY id"
    );
    assert_eq!(got,vec![
        vec![Value::Integer(1),Value::Integer(2),Value::Integer(2),Value::Integer(2)],
        vec![Value::Integer(2),Value::Integer(3),Value::Integer(2),Value::Integer(2)],
        vec![Value::Integer(3),Value::Integer(1),Value::Integer(1),Value::Integer(1)],
        vec![Value::Integer(4),Value::Integer(4),Value::Integer(4),Value::Integer(3)],
        vec![Value::Integer(5),Value::Integer(2),Value::Integer(2),Value::Integer(2)],
        vec![Value::Integer(6),Value::Integer(3),Value::Integer(2),Value::Integer(2)],
        vec![Value::Integer(7),Value::Integer(1),Value::Integer(1),Value::Integer(1)],
    ]);
}

#[test]
fn global_windows_work_with_multiple_order_keys_and_explicit_nulls(){
    assert_eq!(rows("SELECT id, DENSE_RANK() OVER (ORDER BY score ASC NULLS LAST, id DESC) AS dr FROM t ORDER BY id")[3],
        vec![Value::Integer(4),Value::Integer(7)]);
}

#[test]
fn ranking_window_adds_a_blocking_native_stage(){
    let plan=parse_sql("SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t").unwrap();
    let stage=StageGraph::from_plan(&plan).unwrap();
    assert!(stage.stages().iter().any(|s|s.kind==StageKind::Window&&s.kind.is_blocking()));
}

#[test]
fn window_rejects_bad_sort_keys_and_missing_order_by(){
    assert!(parse_sql("SELECT ROW_NUMBER() OVER () FROM t").is_err());
    assert!(parse_sql("SELECT RANK() OVER (PARTITION BY dept) FROM t").is_err());
    let plan=parse_sql("SELECT ROW_NUMBER() OVER (ORDER BY missing) FROM t").unwrap();
    assert!(execute(&plan,&catalog()).is_err());
}

#[test]
fn simple_order_by_uses_spark_default_null_precedence(){
    assert_eq!(rows("SELECT score FROM t ORDER BY score ASC LIMIT 1"),vec![vec![Value::Null]]);
    assert_eq!(rows("SELECT score FROM t ORDER BY score DESC LIMIT 1"),vec![vec![Value::Integer(5)]]);
}

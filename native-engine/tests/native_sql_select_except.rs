use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, plan::Plan, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};
fn catalog()->Catalog {
    let mut c=Catalog::new();
    c.register("t",Batch::from_rows(Schema::new(vec![
        Field::new("id",DataType::Integer,false),
        Field::new("secret",DataType::Text,true),
        Field::new("amount",DataType::Integer,false),
        Field::new("metadata",DataType::Text,true),
    ]).unwrap(),vec![
        vec![Value::Integer(1),Value::Text("key1".into()),Value::Integer(10),Value::Null],
        vec![Value::Integer(2),Value::Text("key2".into()),Value::Integer(20),Value::Text("x".into())],
    ]).unwrap()).unwrap();c
}
fn run(q:&str)->rustspark_native::batch::Batch{
    execute(&parse_sql(q).unwrap(),&catalog()).unwrap()
}
#[test]
fn star_except_prunes_multiple_columns_but_preserves_field_order_and_types(){
    let data=run("SELECT * EXCEPT (secret, metadata) FROM t ORDER BY id");
    assert_eq!(data.schema().fields().iter().map(|f|f.name.as_str()).collect::<Vec<_>>(),vec!["id","amount"]);
    assert_eq!(data.rows().unwrap(),vec![
        vec![Value::Integer(1),Value::Integer(10)],
        vec![Value::Integer(2),Value::Integer(20)],
    ]);
    assert!(!data.schema().fields()[0].nullable);
}
#[test]
fn excluded_columns_remain_available_to_filters_before_projection(){
    assert_eq!(run("SELECT * EXCEPT (secret) FROM t WHERE secret = 'key2'").num_rows(),1);
}
#[test]
fn all_columns_except_keeps_input_row_count(){
    let data=run("SELECT * EXCEPT (id,secret,amount,metadata) FROM t");
    assert_eq!(data.schema().len(),0);
    assert_eq!(data.num_rows(),2);
}
#[test]
fn exclusions_are_case_insensitive_and_must_exist(){
    assert_eq!(run("SELECT * EXCEPT (SeCrEt) FROM t").schema().len(),3);
    let missing=parse_sql("SELECT * EXCEPT (unknown) FROM t").unwrap();
    assert!(execute(&missing,&catalog()).is_err());
    assert!(parse_sql("SELECT * EXCEPT (id, ID) FROM t").is_err());
}
#[test]
fn malformed_exclusions_and_stars_are_rejected(){
    for sql in [
        "SELECT * EXCEPT () FROM t",
        "SELECT * EXCEPT (id,) FROM t",
        "SELECT * EXCEPT (id), amount FROM t",
    ] { assert!(parse_sql(sql).is_err(),"{sql}"); }
}
#[test]
fn pruning_is_a_logical_operator_not_sql_string_replacement(){
    let plan=parse_sql("SELECT * EXCEPT (secret) FROM t").unwrap();
    assert!(matches!(plan,Plan::ProjectExcept{excluded,..}if excluded==vec!["secret"]));
}

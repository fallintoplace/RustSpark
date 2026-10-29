use rustspark_native::{
    batch::Batch,catalog::Catalog,executor::execute,scalar::Value,
    schema::{DataType,Field,Schema},sql::parse_sql,
};
fn fixture()->Catalog{
    let mut c=Catalog::new();
    c.register("numbers",Batch::from_rows(
        Schema::new(vec![Field::new("n",DataType::Integer,false)]).unwrap(),
        vec![vec![Value::Integer(1)],vec![Value::Integer(2)],vec![Value::Integer(3)],
             vec![Value::Integer(4)],vec![Value::Integer(5)]],
    ).unwrap()).unwrap();
    c
}
fn rows(sql:&str)->Vec<Vec<Value>>{
    execute(&parse_sql(sql).unwrap(),&fixture()).unwrap().rows().unwrap()
}
#[test]
fn group_by_expression_and_all_preserve_computed_values() {
    assert_eq!(rows("SELECT n % 2 AS parity, COUNT(*) AS total FROM numbers GROUP BY n % 2 ORDER BY parity"),vec![
        vec![Value::Integer(0),Value::Integer(2)],
        vec![Value::Integer(1),Value::Integer(3)]
    ]);
    assert_eq!(rows("SELECT n % 2 AS parity, COUNT(*) AS total FROM numbers GROUP BY ALL ORDER BY parity"),vec![
        vec![Value::Integer(0),Value::Integer(2)],
        vec![Value::Integer(1),Value::Integer(3)]
    ]);
}
#[test]
fn group_by_ordinal_and_alias_select_the_same_dimensions() {
    for query in [
        "SELECT n % 2 AS parity, COUNT(*) AS total FROM numbers GROUP BY 1 ORDER BY parity",
        "SELECT n % 2 AS parity, COUNT(*) AS total FROM numbers GROUP BY parity ORDER BY parity",
    ] {
        assert_eq!(rows(query),vec![
            vec![Value::Integer(0),Value::Integer(2)],
            vec![Value::Integer(1),Value::Integer(3)]
        ],"{query}");
    }
}
#[test]
fn invalid_grouped_projection_or_ordinal_fails() {
    assert!(parse_sql("SELECT n, COUNT(*) FROM numbers GROUP BY 2").is_err());
    let sql="SELECT n, COUNT(*) FROM numbers GROUP BY n % 2";
    assert!(parse_sql(sql).is_err());
}

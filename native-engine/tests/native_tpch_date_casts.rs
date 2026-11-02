use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

#[test]
fn tpch_date_casts_validate_real_calendar_dates() {
    let catalog = Catalog::new();
    for expression in ["CAST('1998-12-01' AS DATE)", "TO_DATE('1998-12-01')"] {
        let sql = format!("SELECT {expression}");
        assert_eq!(execute(&parse_sql(&sql).unwrap(), &catalog).unwrap().rows().unwrap(),
            vec![vec![Value::Text("1998-12-01".into())]]);
    }
    assert_eq!(execute(&parse_sql("SELECT TRY_CAST('1998-02-30' AS DATE)").unwrap(), &catalog)
        .unwrap().rows().unwrap(), vec![vec![Value::Null]]);
    assert!(execute(&parse_sql("SELECT CAST('1998-02-30' AS DATE)").unwrap(), &catalog)
        .is_err());
}

#[test]
fn date_cast_on_nullable_columns_keeps_null_semantics() {
    let mut catalog = Catalog::new();
    catalog.register("t", Batch::from_rows(
        Schema::new(vec![Field::new("date_text", DataType::Text, true)]).unwrap(),
        vec![
            vec![Value::Text("2000-02-29".into())],
            vec![Value::Text("1900-02-29".into())],
            vec![Value::Null],
        ]
    ).unwrap()).unwrap();
    assert_eq!(execute(&parse_sql("SELECT TRY_CAST(date_text AS DATE) FROM t").unwrap(), &catalog)
        .unwrap().rows().unwrap(), vec![
        vec![Value::Text("2000-02-29".into())],
        vec![Value::Null],
        vec![Value::Null],
    ]);
}

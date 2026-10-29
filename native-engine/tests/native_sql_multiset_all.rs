use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, plan::Plan, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let schema = Schema::new(vec![Field::new("value", DataType::Integer, true)]).unwrap();
    let left = [1, 1, 2, 2, 2, 4].into_iter()
        .map(|n| vec![Value::Integer(n)]).chain([
            vec![Value::Null], vec![Value::Null],
        ]).collect();
    let right = [1, 2, 2, 3].into_iter()
        .map(|n| vec![Value::Integer(n)]).chain([vec![Value::Null]]).collect();
    let mut result = Catalog::new();
    result.register("a", Batch::from_rows(schema.clone(), left).unwrap()).unwrap();
    result.register("b", Batch::from_rows(schema, right).unwrap()).unwrap();
    result
}

fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &catalog()).unwrap().rows().unwrap()
}

#[test]
fn all_set_operators_preserve_row_multiplicity_including_nulls() {
    let intersection = rows("SELECT * FROM a INTERSECT ALL SELECT * FROM b");
    assert_eq!(intersection, vec![
        vec![Value::Integer(1)], vec![Value::Integer(2)],
        vec![Value::Integer(2)], vec![Value::Null],
    ]);
    let subtraction = rows("SELECT * FROM a EXCEPT ALL SELECT * FROM b");
    assert_eq!(subtraction, vec![
        vec![Value::Integer(1)], vec![Value::Integer(2)],
        vec![Value::Integer(4)], vec![Value::Null],
    ]);
    assert_eq!(rows("SELECT * FROM a MINUS ALL SELECT * FROM b"), subtraction);
    assert_eq!(rows("SELECT * FROM a EXCEPT DISTINCT SELECT * FROM b"),
        vec![vec![Value::Integer(4)]]);
    assert_eq!(rows("SELECT * FROM a INTERSECT DISTINCT SELECT * FROM b"),
        vec![vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Null]]);
}

#[test]
fn intersect_binds_more_tightly_than_except_and_union() {
    let parsed = parse_sql(
        "SELECT * FROM a EXCEPT ALL SELECT * FROM a INTERSECT ALL SELECT * FROM b"
    ).unwrap();
    assert!(matches!(parsed, Plan::ExceptAll { right, .. }
        if matches!(*right, Plan::IntersectAll { .. })));
    let parsed = parse_sql(
        "SELECT * FROM a INTERSECT ALL SELECT * FROM b UNION ALL SELECT * FROM a"
    ).unwrap();
    assert!(matches!(parsed, Plan::UnionAll { left, .. }
        if matches!(*left, Plan::IntersectAll { .. })));
}

#[test]
fn invalid_set_operator_modifiers_are_not_ignored() {
    assert!(parse_sql("SELECT * FROM a EXCEPT ALL DISTINCT SELECT * FROM b").is_err());
    assert!(parse_sql("SELECT * FROM a INTERSECT ALL ALL SELECT * FROM b").is_err());
    assert!(parse_sql("SELECT * FROM a MINUS DISTINCT ALL SELECT * FROM b").is_err());
}

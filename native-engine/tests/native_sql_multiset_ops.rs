use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, plan::Plan, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let schema = Schema::new(vec![Field::new("k", DataType::Integer, true)]).unwrap();
    let mut catalog = Catalog::new();
    for (table, values) in [
        ("a", vec![Some(1), Some(1), Some(1), None, None, Some(3)]),
        ("b", vec![Some(1), Some(1), None, Some(2)]),
        ("c", vec![Some(1), None]),
    ] {
        catalog.register(table, Batch::from_rows(schema.clone(),
            values.into_iter().map(|value| vec![value.map(Value::Integer).unwrap_or(Value::Null)]).collect()
        ).unwrap()).unwrap();
    }
    catalog
}

fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &catalog()).unwrap().rows().unwrap()
}

#[test]
fn intersect_all_keeps_exact_multiplicity_including_null() {
    assert_eq!(
        rows("SELECT k FROM a INTERSECT ALL SELECT k FROM b"),
        vec![vec![Value::Integer(1)], vec![Value::Integer(1)], vec![Value::Null]],
    );
    assert_eq!(
        rows("SELECT k FROM a INTERSECT DISTINCT SELECT k FROM b"),
        vec![vec![Value::Integer(1)], vec![Value::Null]],
    );
    assert_eq!(
        rows("SELECT k FROM a INTERSECT SELECT k FROM b"),
        vec![vec![Value::Integer(1)], vec![Value::Null]],
    );
}

#[test]
fn except_all_subtracts_duplicates_including_null() {
    assert_eq!(
        rows("SELECT k FROM a EXCEPT ALL SELECT k FROM b"),
        vec![vec![Value::Integer(1)], vec![Value::Null], vec![Value::Integer(3)]],
    );
    assert_eq!(
        rows("SELECT k FROM a EXCEPT DISTINCT SELECT k FROM b"),
        vec![vec![Value::Integer(3)]],
    );
    assert_eq!(
        rows("SELECT k FROM a EXCEPT SELECT k FROM b"),
        vec![vec![Value::Integer(3)]],
    );
}

#[test]
fn intersect_has_higher_precedence_than_except_and_all_is_not_lost_in_optimization() {
    let query = "SELECT k FROM a INTERSECT ALL SELECT k FROM b EXCEPT ALL SELECT k FROM c";
    let plan = parse_sql(query).unwrap();
    assert!(matches!(&plan, Plan::Except { all: true, left, .. }
        if matches!(left.as_ref(), Plan::Intersect { all: true, .. })));
    assert_eq!(execute(&plan, &catalog()).unwrap().rows().unwrap(),
        vec![vec![Value::Integer(1)]]);
}

#[test]
fn set_all_rejects_incompatible_schemas_and_missing_inputs() {
    let mut catalog = catalog();
    catalog.register("text", Batch::from_rows(
        Schema::new(vec![Field::new("k", DataType::Text, false)]).unwrap(),
        vec![vec![Value::Text("1".into())]],
    ).unwrap()).unwrap();
    let plan = parse_sql("SELECT k FROM a INTERSECT ALL SELECT k FROM text").unwrap();
    assert!(execute(&plan, &catalog).is_err());
    assert!(parse_sql("SELECT k FROM a EXCEPT ALL").is_err());
}

#[test]
fn optimizer_never_converts_all_into_distinct() {
    let plan = parse_sql("SELECT k FROM a EXCEPT ALL SELECT k FROM b").unwrap();
    let optimized = rustspark_native::optimizer::optimize(plan);
    assert!(matches!(optimized, Plan::Except { all: true, .. }));
}

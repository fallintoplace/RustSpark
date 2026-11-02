use rustspark_native::{
    batch::Batch, expr::Expr, scalar::Value,
    schema::{DataType, Field, Schema},
    sort::SortKey,
    window_offsets::{offset_batch, OffsetDirection},
};

fn input() -> Batch {
    let schema = Schema::new(vec![
        Field::new("grp", DataType::Text, false),
        Field::new("ordinal", DataType::Integer, false),
        Field::new("value", DataType::Integer, true),
    ]).unwrap();
    Batch::from_rows(schema, vec![
        vec![Value::Text("a".into()), Value::Integer(2), Value::Integer(20)],
        vec![Value::Text("a".into()), Value::Integer(1), Value::Null],
        vec![Value::Text("b".into()), Value::Integer(1), Value::Integer(50)],
        vec![Value::Text("a".into()), Value::Integer(3), Value::Integer(30)],
        vec![Value::Text("a".into()), Value::Integer(4), Value::Null],
    ]).unwrap()
}

fn shift(direction: OffsetDirection, ignore_nulls: bool) -> Vec<Value> {
    let keys = [SortKey::ascending(Expr::Column("ordinal".into()))];
    offset_batch(input(), &["grp"], &keys, "value", direction, 1,
        Value::Integer(-1), ignore_nulls, "shift").unwrap()
        .column(3).unwrap().to_vec()
}

#[test]
fn null_aware_lag_and_lead_preserve_partition_boundaries() {
    assert_eq!(shift(OffsetDirection::Lag, false), vec![
        Value::Null, Value::Integer(-1), Value::Integer(-1),
        Value::Integer(20), Value::Integer(30),
    ]);
    assert_eq!(shift(OffsetDirection::Lag, true), vec![
        Value::Integer(-1), Value::Integer(-1), Value::Integer(-1),
        Value::Integer(20), Value::Integer(30),
    ]);
    assert_eq!(shift(OffsetDirection::Lead, false), vec![
        Value::Integer(30), Value::Integer(20), Value::Integer(-1),
        Value::Null, Value::Integer(-1),
    ]);
    assert_eq!(shift(OffsetDirection::Lead, true), vec![
        Value::Integer(30), Value::Integer(20), Value::Integer(-1),
        Value::Integer(-1), Value::Integer(-1),
    ]);
}

#[test]
fn offset_zero_and_large_offsets_handle_empty_and_singleton_partitions() {
    let result = offset_batch(input(), &["grp"], &[], "value",
        OffsetDirection::Lag, 0, Value::Integer(-1), true, "same").unwrap();
    assert_eq!(result.value(1, 3).unwrap(), &Value::Null);
    let result = offset_batch(input(), &["grp"], &[], "value",
        OffsetDirection::Lead, 10, Value::Integer(-1), true, "none").unwrap();
    assert!(result.column(3).unwrap().iter()
        .all(|v| v == &Value::Integer(-1)));
    let empty = Batch::empty(Schema::new(vec![
        Field::new("value", DataType::Integer, true)
    ]).unwrap());
    assert_eq!(offset_batch(empty, &[], &[], "value", OffsetDirection::Lag,
        1, Value::Null, false, "previous").unwrap().num_rows(), 0);
}

#[test]
fn window_offsets_reject_invalid_types_missing_columns_and_aliases() {
    assert!(offset_batch(input(), &[], &[], "value", OffsetDirection::Lag,
        1, Value::Text("bad".into()), false, "lag").is_err());
    assert!(offset_batch(input(), &[], &[], "missing", OffsetDirection::Lag,
        1, Value::Null, false, "lag").is_err());
    assert!(offset_batch(input(), &[], &[], "value", OffsetDirection::Lag,
        1, Value::Null, false, "value").is_err());
    assert!(offset_batch(input(), &[], &[], "value", OffsetDirection::Lag,
        usize::MAX, Value::Null, false, "lag").is_err());
}

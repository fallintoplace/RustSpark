use rustspark_native::{
    batch::Batch, expr::Expr, scalar::Value,
    schema::{DataType, Field, Schema},
    sort::SortKey, window_buckets::ntile_batch,
};

fn input() -> Batch {
    let schema = Schema::new(vec![
        Field::new("part", DataType::Text, false),
        Field::new("key", DataType::Integer, true),
    ]).unwrap();
    Batch::from_rows(schema, vec![
        vec![Value::Text("a".into()), Value::Integer(2)],
        vec![Value::Text("b".into()), Value::Integer(5)],
        vec![Value::Text("a".into()), Value::Integer(1)],
        vec![Value::Text("a".into()), Value::Integer(2)],
        vec![Value::Text("b".into()), Value::Null],
        vec![Value::Text("a".into()), Value::Integer(4)],
        vec![Value::Text("a".into()), Value::Integer(3)],
    ]).unwrap()
}

#[test]
fn ntile_balances_buckets_per_partition_and_preserves_original_order() {
    let sort = [SortKey {
        expression: Expr::Column("key".into()),
        descending: false, nulls_first: false,
    }];
    let result = ntile_batch(input(), &["part"], &sort, 2, "bucket").unwrap();
    // Partition a: 1,2,2 (bucket 1), 3,4 (bucket 2).
    // Partition b: 5 (bucket 1), NULL (bucket 2).
    assert_eq!(result.column(2).unwrap(), &[
        Value::Integer(1), Value::Integer(1), Value::Integer(1),
        Value::Integer(1), Value::Integer(2), Value::Integer(2),
        Value::Integer(2),
    ]);
}

#[test]
fn ntile_more_buckets_than_rows_and_singletons_have_no_gaps() {
    let result = ntile_batch(input(), &["part"], &[], 100, "tile").unwrap();
    assert_eq!(result.column(2).unwrap(), &[
        Value::Integer(1), Value::Integer(1), Value::Integer(2),
        Value::Integer(3), Value::Integer(2), Value::Integer(4),
        Value::Integer(5),
    ]);
    let schema = Schema::new(vec![Field::new("x", DataType::Integer, false)]).unwrap();
    let single = Batch::from_rows(schema.clone(), vec![vec![Value::Integer(7)]]).unwrap();
    assert_eq!(ntile_batch(single, &[], &[], 3, "tile").unwrap()
        .value(0, 1).unwrap(), &Value::Integer(1));
    assert_eq!(ntile_batch(Batch::empty(schema), &[], &[], 2, "tile")
        .unwrap().num_rows(), 0);
}

#[test]
fn ntile_rejects_invalid_counts_keys_and_aliases() {
    let keys = [SortKey::ascending(Expr::Column("missing".into()))];
    assert!(ntile_batch(input(), &[], &[], 0, "tile").is_err());
    assert!(ntile_batch(input(), &[], &[], usize::MAX, "tile").is_err());
    assert!(ntile_batch(input(), &[], &keys, 2, "tile").is_err());
    assert!(ntile_batch(input(), &[], &[], 2, "key").is_err());
}

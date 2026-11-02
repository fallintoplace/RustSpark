use rustspark_native::{
    batch::Batch, join::{hash_join, hash_join_composite, JoinKind},
    scalar::Value, schema::{DataType, Field, Schema},
};

fn side(values: Vec<Vec<Value>>, names: [&str; 3]) -> Batch {
    Batch::from_rows(Schema::new(vec![
        Field::new(names[0], DataType::Integer, true),
        Field::new(names[1], DataType::Text, true),
        Field::new(names[2], DataType::Integer, true),
    ]).unwrap(), values).unwrap()
}

#[test]
fn composite_keys_filter_partial_matches_and_preserve_outer_rows() {
    let lhs = side(vec![
        vec![Value::Integer(1), Value::Text("eu".into()), Value::Integer(10)],
        vec![Value::Integer(1), Value::Text("us".into()), Value::Integer(20)],
        vec![Value::Null, Value::Text("eu".into()), Value::Integer(30)],
    ], ["id", "region", "payload"]);
    let rhs = side(vec![
        vec![Value::Integer(1), Value::Text("eu".into()), Value::Integer(100)],
        vec![Value::Integer(1), Value::Text("eu".into()), Value::Integer(101)],
        vec![Value::Integer(2), Value::Text("eu".into()), Value::Integer(200)],
    ], ["rid", "zone", "amount"]);
    let inner = hash_join_composite(
        &lhs, &rhs, &["id", "region"], &["rid", "zone"], JoinKind::Inner,
    ).unwrap();
    assert_eq!(inner.num_rows(), 2);
    assert_eq!(inner.row(0).unwrap()[5], Value::Integer(100));
    assert_eq!(inner.row(1).unwrap()[5], Value::Integer(101));
    assert_eq!(hash_join_composite(&lhs, &rhs, &["id", "region"],
        &["rid", "zone"], JoinKind::Left).unwrap().num_rows(), 4);
    assert_eq!(hash_join_composite(&lhs, &rhs, &["id", "region"],
        &["rid", "zone"], JoinKind::Right).unwrap().num_rows(), 3);
    assert_eq!(hash_join_composite(&lhs, &rhs, &["id", "region"],
        &["rid", "zone"], JoinKind::Full).unwrap().num_rows(), 5);
    assert_eq!(hash_join_composite(&lhs, &rhs, &["id", "region"],
        &["rid", "zone"], JoinKind::Semi).unwrap().num_rows(), 1);
    assert_eq!(hash_join_composite(&lhs, &rhs, &["id", "region"],
        &["rid", "zone"], JoinKind::Anti).unwrap().num_rows(), 2);
    let single = hash_join_composite(&lhs, &rhs, &["id"], &["rid"], JoinKind::Inner).unwrap();
    assert_eq!(single, hash_join(&lhs, &rhs, "id", "rid", JoinKind::Inner).unwrap());
}

#[test]
fn rejects_mismatched_composite_key_types_and_arities() {
    let side = side(vec![], ["a", "b", "c"]);
    for (a, b) in [
        (vec![], vec![]), (vec!["a"], vec!["a", "b"]),
        (vec!["a", "b"], vec!["a", "a"]),
    ] {
        assert!(hash_join_composite(&side, &side, &a, &b, JoinKind::Inner).is_err());
    }
}

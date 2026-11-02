use rustspark_native::{
    batch::Batch, expr::Expr, scalar::Value,
    schema::{DataType, Field, Schema},
    sort::SortKey, window::{rank_batch, Ranking},
};
fn example() -> Batch {
    let schema = Schema::new(vec![
        Field::new("group", DataType::Text, true),
        Field::new("score", DataType::Integer, true),
    ]).unwrap();
    Batch::from_rows(schema, vec![
        vec![Value::Text("x".into()), Value::Integer(2)],
        vec![Value::Text("y".into()), Value::Integer(4)],
        vec![Value::Text("x".into()), Value::Integer(1)],
        vec![Value::Text("x".into()), Value::Integer(2)],
        vec![Value::Text("y".into()), Value::Null],
    ]).unwrap()
}
fn scores(ranking: Ranking) -> Vec<Value> {
    let batch = rank_batch(example(), &["group"], &[
        SortKey { expression: Expr::Column("score".into()),
            descending: false, nulls_first: false },
    ], ranking, "position").unwrap();
    assert_eq!(batch.schema().len(), 3);
    batch.column(2).unwrap().to_vec()
}
#[test]
fn partitioned_row_number_preserves_original_rows() {
    assert_eq!(scores(Ranking::RowNumber), vec![
        Value::Integer(2), Value::Integer(1), Value::Integer(1),
        Value::Integer(3), Value::Integer(2),
    ]);
}
#[test]
fn partitioned_rank_and_dense_rank_handle_ties_and_nulls() {
    assert_eq!(scores(Ranking::Rank), vec![
        Value::Integer(2), Value::Integer(1), Value::Integer(1),
        Value::Integer(2), Value::Integer(2),
    ]);
    assert_eq!(scores(Ranking::DenseRank), vec![
        Value::Integer(2), Value::Integer(1), Value::Integer(1),
        Value::Integer(2), Value::Integer(2),
    ]);
}
#[test]
fn invalid_window_keys_and_duplicate_output_names_are_rejected() {
    let key = SortKey::ascending(Expr::Column("missing".into()));
    assert!(rank_batch(example(), &[], &[key], Ranking::Rank, "r").is_err());
    assert!(rank_batch(example(), &[], &[], Ranking::Rank, "score").is_err());
}

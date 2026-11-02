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


#[test]
fn distributions_preserve_partition_ties_nulls_and_row_order() {
    use rustspark_native::window::{distribution_batch, Distribution};
    let keys = [SortKey {
        expression: Expr::Column("score".into()),
        descending: false, nulls_first: false,
    }];
    let percent = distribution_batch(
        example(), &["group"], &keys, Distribution::PercentRank, "pct"
    ).unwrap();
    assert_eq!(percent.column(2).unwrap(), &[
        Value::Float(0.5), Value::Float(0.0), Value::Float(0.0),
        Value::Float(0.5), Value::Float(1.0),
    ]);
    let cumulative = distribution_batch(
        example(), &["group"], &keys, Distribution::CumeDist, "cume"
    ).unwrap();
    assert_eq!(cumulative.column(2).unwrap(), &[
        Value::Float(1.0), Value::Float(0.5), Value::Float(1.0 / 3.0),
        Value::Float(1.0), Value::Float(1.0),
    ]);
}

#[test]
fn distributions_handle_singletons_empty_batches_and_peer_only_partitions() {
    use rustspark_native::window::{distribution_batch, Distribution};
    let schema = Schema::new(vec![Field::new("key", DataType::Integer, false)]).unwrap();
    for distribution in [Distribution::PercentRank, Distribution::CumeDist] {
        let single = Batch::from_rows(schema.clone(),
            vec![vec![Value::Integer(7)]]).unwrap();
        let output = distribution_batch(single, &[], &[], distribution, "stat").unwrap();
        assert_eq!(output.value(0, 1).unwrap(),
            &Value::Float(if distribution == Distribution::PercentRank { 0.0 } else { 1.0 }));

        let empty = Batch::empty(schema.clone());
        let output = distribution_batch(empty, &[], &[], distribution, "stat").unwrap();
        assert_eq!(output.num_rows(), 0);
        assert_eq!(output.schema().len(), 2);
    }
    let tied = Batch::from_rows(schema, vec![
        vec![Value::Integer(2)], vec![Value::Integer(3)],
    ]).unwrap();
    let output = distribution_batch(tied, &[], &[],
        Distribution::CumeDist, "stat").unwrap();
    assert_eq!(output.column(1).unwrap(), &[Value::Float(1.0), Value::Float(1.0)]);
}

#[test]
fn distributions_reject_invalid_keys_and_duplicate_names() {
    use rustspark_native::window::{distribution_batch, Distribution};
    assert!(distribution_batch(example(), &[], &[
        SortKey::ascending(Expr::Column("missing".into()))
    ], Distribution::PercentRank, "pct").is_err());
    assert!(distribution_batch(example(), &[], &[],
        Distribution::CumeDist, "score").is_err());
}

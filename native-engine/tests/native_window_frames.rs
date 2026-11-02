use rustspark_native::{
    batch::Batch, expr::Expr, scalar::Value,
    schema::{DataType, Field, Schema}, sort::SortKey,
    window_frames::{aggregate_rows_frame, FrameAggregate},
};

fn sample() -> Batch {
    let schema = Schema::new(vec![
        Field::new("grp", DataType::Text, false),
        Field::new("position", DataType::Integer, false),
        Field::new("amount", DataType::Integer, true),
    ]).unwrap();
    Batch::from_rows(schema, vec![
        vec![Value::Text("a".into()), Value::Integer(3), Value::Integer(4)],
        vec![Value::Text("a".into()), Value::Integer(1), Value::Integer(1)],
        vec![Value::Text("b".into()), Value::Integer(2), Value::Integer(20)],
        vec![Value::Text("a".into()), Value::Integer(2), Value::Null],
        vec![Value::Text("b".into()), Value::Integer(1), Value::Integer(10)],
        vec![Value::Text("a".into()), Value::Integer(4), Value::Integer(5)],
    ]).unwrap()
}

fn values(aggregate: FrameAggregate, preceding: usize, following: usize) -> Vec<Value> {
    aggregate_rows_frame(sample(), &["grp"], &[
        SortKey::ascending(Expr::Column("position".into()))
    ], "amount", preceding, following, aggregate, "window").unwrap()
        .column(3).unwrap().to_vec()
}

#[test]
fn sliding_sum_avg_and_count_restore_unsorted_original_rows() {
    assert_eq!(values(FrameAggregate::Sum, 1, 1), vec![
        Value::Integer(9), Value::Integer(1), Value::Integer(30),
        Value::Integer(5), Value::Integer(30), Value::Integer(9),
    ]);
    assert_eq!(values(FrameAggregate::Average, 1, 1), vec![
        Value::Float(4.5), Value::Float(1.0), Value::Float(15.0),
        Value::Float(2.5), Value::Float(15.0), Value::Float(4.5),
    ]);
    assert_eq!(values(FrameAggregate::Count, 1, 1), vec![
        Value::Integer(2), Value::Integer(1), Value::Integer(2),
        Value::Integer(2), Value::Integer(2), Value::Integer(2),
    ]);
}

#[test]
fn supports_single_row_frames_empty_results_and_null_only_frames() {
    assert_eq!(values(FrameAggregate::Sum, 0, 0)[3], Value::Null);
    assert_eq!(values(FrameAggregate::Average, 0, 0)[3], Value::Null);
    assert_eq!(values(FrameAggregate::Count, 0, 0)[3], Value::Integer(0));
    assert_eq!(values(FrameAggregate::Sum, usize::MAX, usize::MAX), vec![
        Value::Integer(10), Value::Integer(10), Value::Integer(30),
        Value::Integer(10), Value::Integer(30), Value::Integer(10),
    ]);
    let schema = Schema::new(vec![Field::new("amount", DataType::Float, true)]).unwrap();
    assert_eq!(aggregate_rows_frame(Batch::empty(schema), &[], &[], "amount",
        1, 1, FrameAggregate::Sum, "sum").unwrap().num_rows(), 0);
}

#[test]
fn rejects_integer_overflow_and_nonnumeric_sums() {
    let schema = Schema::new(vec![
        Field::new("amount", DataType::Integer, false),
    ]).unwrap();
    let input = Batch::from_rows(schema.clone(), vec![
        vec![Value::Integer(i64::MAX)], vec![Value::Integer(1)],
    ]).unwrap();
    assert!(aggregate_rows_frame(input, &[], &[], "amount", 1, 0,
        FrameAggregate::Sum, "sum").is_err());
    let text = Schema::new(vec![Field::new("amount", DataType::Text, false)]).unwrap();
    let input = Batch::from_rows(text, vec![vec![Value::Text("hello".into())]]).unwrap();
    assert!(aggregate_rows_frame(input, &[], &[], "amount", 1, 0,
        FrameAggregate::Sum, "sum").is_err());
    assert!(aggregate_rows_frame(Batch::empty(schema), &[], &[], "amount", 1, 0,
        FrameAggregate::Sum, "amount").is_err());
}

use rustspark_native::{
    batch::Batch, scalar::Value, schema::{DataType, Field, Schema},
    spill_ipc::{spill, spill_stream},
};

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Integer, true),
        Field::new("label", DataType::Text, true),
    ]).unwrap()
}

fn batch(rows: usize) -> Batch {
    let schema = schema();
    Batch::from_rows(schema, (0..rows).map(|i| vec![
        Value::Integer(i as i64), Value::Text(format!("row-{i}")),
    ]).collect()).unwrap()
}

#[test]
fn verified_spill_validates_schema_before_reading() {
    let expected = schema();
    let file = spill(std::env::temp_dir(), &expected, &[batch(2)]).unwrap();
    let bad = Schema::new(vec![
        Field::new("different", DataType::Integer, true),
        Field::new("label", DataType::Text, true),
    ]).unwrap();
    assert!(file.read_verified(&bad, 10, 10).is_err());
    let values = file.read_verified(&expected, 1, 2).unwrap()
        .collect::<rustspark_native::Result<Vec<_>>>().unwrap();
    assert_eq!(values, vec![batch(2)]);
}

#[test]
fn verifies_total_rows_and_batches_without_returning_excess_data() {
    let expected = schema();
    let file = spill_stream(std::env::temp_dir(), &expected,
        [Ok(batch(1)), Ok(batch(2)), Ok(batch(3))]).unwrap();
    let mut by_rows = file.read_verified(&expected, 3, 2).unwrap();
    assert_eq!(by_rows.next().unwrap().unwrap(), batch(1));
    assert!(by_rows.next().unwrap().is_err());
    assert_eq!((by_rows.rows_seen(), by_rows.batches_seen()), (1, 1));
    assert!(by_rows.next().is_none());

    let mut by_batches = file.read_verified(&expected, 2, 100).unwrap();
    assert_eq!(by_batches.next().unwrap().unwrap(), batch(1));
    assert_eq!(by_batches.next().unwrap().unwrap(), batch(2));
    assert!(by_batches.next().unwrap().is_err());
    assert!(by_batches.next().is_none());
}

#[test]
fn zero_limits_allow_only_empty_spill_streams() {
    let expected = schema();
    let empty = spill(std::env::temp_dir(), &expected, &[]).unwrap();
    assert!(empty.read_verified(&expected, 0, 0).unwrap().next().is_none());
    let file = spill(std::env::temp_dir(), &expected, &[batch(1)]).unwrap();
    assert!(file.read_verified(&expected, 0, 0).unwrap().next().unwrap().is_err());
    let file_path = file.path().to_path_buf();
    let reader = file.read_verified(&expected, 1, 1).unwrap();
    drop(file);
    assert!(file_path.exists());
    assert_eq!(reader.count(), 1);
    assert!(!file_path.exists());
}

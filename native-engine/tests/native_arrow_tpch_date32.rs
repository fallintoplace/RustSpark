use std::sync::Arc;
use arrow_array::{Date32Array, RecordBatch, ArrayRef};
use rustspark_native::{arrow_bridge::{from_arrow, schema_from_arrow}, scalar::Value, schema::DataType};

#[test]
fn tpch_dates_decode_as_sortable_iso_text() {
    let input = RecordBatch::try_from_iter(vec![
        ("l_shipdate", Arc::new(Date32Array::from(vec![
            Some(0), Some(1), Some(9_556), Some(9_920), None,
        ])) as ArrayRef),
    ]).unwrap();
    let schema = schema_from_arrow(input.schema().as_ref()).unwrap();
    assert_eq!(schema.fields()[0].data_type, DataType::Text);
    let output = from_arrow(&input).unwrap();
    assert_eq!(output.value(0, 0).unwrap(), &Value::Text("1970-01-01".into()));
    assert_eq!(output.value(1, 0).unwrap(), &Value::Text("1970-01-02".into()));
    assert_eq!(output.value(4, 0).unwrap(), &Value::Null);
}

#[test]
fn leap_days_and_pre_epoch_dates_convert_correctly() {
    let input = RecordBatch::try_from_iter(vec![
        ("d", Arc::new(Date32Array::from(vec![
            Some(-1), Some(11_016), Some(18_321),
        ])) as ArrayRef),
    ]).unwrap();
    let output = from_arrow(&input).unwrap();
    assert_eq!(output.value(0, 0).unwrap(), &Value::Text("1969-12-31".into()));
    assert_eq!(output.value(1, 0).unwrap(), &Value::Text("2000-02-29".into()));
    assert_eq!(output.value(2, 0).unwrap(), &Value::Text("2020-02-29".into()));
}

#[test]
fn rejects_dates_outside_native_iso_range() {
    let input = RecordBatch::try_from_iter(vec![
        ("d", Arc::new(Date32Array::from(vec![i32::MAX])) as ArrayRef),
    ]).unwrap();
    assert!(from_arrow(&input).is_err());
}

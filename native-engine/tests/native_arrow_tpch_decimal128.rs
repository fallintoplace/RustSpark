use std::sync::Arc;
use arrow_array::{ArrayRef, Decimal128Array, RecordBatch};
use arrow_schema::{DataType as ArrowType, Field as ArrowField, Schema as ArrowSchema};
use rustspark_native::{arrow_bridge::{from_arrow, schema_from_arrow}, scalar::Value, schema::DataType};

#[test]
fn tpch_parquet_prices_and_discounts_decode_as_float_bridge() {
    let amounts = Decimal128Array::from(vec![Some(10_050_i128), None, Some(-75_i128)])
        .with_precision_and_scale(15, 2).unwrap();
    let batch = RecordBatch::try_from_iter(vec![
        ("l_extendedprice", Arc::new(amounts) as ArrayRef),
    ]).unwrap();
    let schema = schema_from_arrow(batch.schema().as_ref()).unwrap();
    assert_eq!(schema.fields()[0].data_type, DataType::Float);
    let output = from_arrow(&batch).unwrap();
    assert_eq!(output.value(0, 0).unwrap(), &Value::Float(100.50));
    assert_eq!(output.value(1, 0).unwrap(), &Value::Null);
    assert_eq!(output.value(2, 0).unwrap(), &Value::Float(-0.75));
}

#[test]
fn wide_decimals_do_not_silently_lose_arbitrary_precision() {
    let schema = ArrowSchema::new(vec![
        ArrowField::new("n", ArrowType::Decimal128(38, 2), true),
    ]);
    assert!(schema_from_arrow(&schema).is_err());
}

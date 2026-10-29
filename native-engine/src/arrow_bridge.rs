//! Convert native batches to and from Apache Arrow without DataFusion.
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array,
    LargeStringArray, RecordBatch, StringArray, UInt64Array,
    builder::StringBuilder,
};
use arrow_schema::{DataType as ArrowType, Field as ArrowField, Schema as ArrowSchema};

use crate::{
    batch::Batch,
    scalar::Value,
    schema::{DataType, Field, Schema},
    Error, Result,
};

pub fn to_arrow(batch: &Batch) -> Result<RecordBatch> {
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        let input = batch.column(index)?;
        match field.data_type {
            DataType::Boolean => {
                let values = input.iter().map(|value| match value {
                    Value::Null => Ok(None),
                    Value::Boolean(v) => Ok(Some(*v)),
                    _ => Err(Error::Type("invalid boolean column".into())),
                }).collect::<Result<Vec<_>>>()?;
                columns.push(Arc::new(BooleanArray::from(values)));
                fields.push(ArrowField::new(&field.name, ArrowType::Boolean, field.nullable));
            }
            DataType::Integer => {
                let values = input.iter().map(|value| match value {
                    Value::Null => Ok(None),
                    Value::Integer(v) => Ok(Some(*v)),
                    _ => Err(Error::Type("invalid integer column".into())),
                }).collect::<Result<Vec<_>>>()?;
                columns.push(Arc::new(Int64Array::from(values)));
                fields.push(ArrowField::new(&field.name, ArrowType::Int64, field.nullable));
            }
            DataType::Float => {
                let values = input.iter().map(|value| match value {
                    Value::Null => Ok(None),
                    Value::Float(v) => Ok(Some(*v)),
                    _ => Err(Error::Type("invalid float column".into())),
                }).collect::<Result<Vec<_>>>()?;
                columns.push(Arc::new(Float64Array::from(values)));
                fields.push(ArrowField::new(&field.name, ArrowType::Float64, field.nullable));
            }
            DataType::Text => {
                let text_bytes = input.iter().fold(0usize, |total, value| match value {
                    Value::Text(value) => total.saturating_add(value.len()),
                    _ => total,
                });
                let mut builder = StringBuilder::with_capacity(input.len(), text_bytes);
                for value in input {
                    match value {
                        Value::Null => builder.append_null(),
                        Value::Text(v) => builder.append_value(v),
                        _ => return Err(Error::Type("invalid string column".into())),
                    }
                }
                columns.push(Arc::new(builder.finish()));
                fields.push(ArrowField::new(&field.name, ArrowType::Utf8, field.nullable));
            }
        }
    }
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)
        .map_err(|err| Error::Execution(format!("Arrow conversion failed: {err}")))
}

pub fn from_arrow(input: &RecordBatch) -> Result<Batch> {
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for (index, field) in input.schema().fields().iter().enumerate() {
        let array = input.column(index);
        let data_type = match field.data_type() {
            ArrowType::Boolean => DataType::Boolean,
            ArrowType::Int8 | ArrowType::Int16 | ArrowType::Int32 | ArrowType::Int64 |
            ArrowType::UInt8 | ArrowType::UInt16 | ArrowType::UInt32 |
            ArrowType::UInt64 => DataType::Integer,
            ArrowType::Float32 | ArrowType::Float64 => DataType::Float,
            ArrowType::Utf8 | ArrowType::LargeUtf8 => DataType::Text,
            other => return Err(Error::Unsupported(format!("unsupported Arrow type: {other}"))),
        };
        fields.push(Field::new(field.name(), data_type, field.is_nullable()));
        // Decode common arrays with a single downcast per column rather than per row.
        let common: Option<Vec<Value>> = match field.data_type() {
            ArrowType::Boolean => {
                let values = array.as_any().downcast_ref::<BooleanArray>()
                    .ok_or_else(|| Error::Type("invalid Boolean Arrow array".into()))?;
                Some((0..values.len()).map(|row| {
                    if values.is_null(row) { Value::Null }
                    else { Value::Boolean(values.value(row)) }
                }).collect())
            }
            ArrowType::Int64 => {
                let values = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| Error::Type("invalid Int64 Arrow array".into()))?;
                Some((0..values.len()).map(|row| {
                    if values.is_null(row) { Value::Null }
                    else { Value::Integer(values.value(row)) }
                }).collect())
            }
            ArrowType::Float64 => {
                let values = array.as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| Error::Type("invalid Float64 Arrow array".into()))?;
                Some((0..values.len()).map(|row| {
                    if values.is_null(row) { Value::Null }
                    else { Value::Float(values.value(row)) }
                }).collect())
            }
            ArrowType::Utf8 => {
                let values = array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| Error::Type("invalid Utf8 Arrow array".into()))?;
                Some((0..values.len()).map(|row| {
                    if values.is_null(row) { Value::Null }
                    else { Value::Text(values.value(row).into()) }
                }).collect())
            }
            ArrowType::LargeUtf8 => {
                let values = array.as_any().downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| Error::Type("invalid LargeUtf8 Arrow array".into()))?;
                Some((0..values.len()).map(|row| {
                    if values.is_null(row) { Value::Null }
                    else { Value::Text(values.value(row).into()) }
                }).collect())
            }
            _ => None,
        };
        if let Some(values) = common {
            columns.push(values);
            continue;
        }
        let mut values = Vec::with_capacity(input.num_rows());
        for row in 0..input.num_rows() {
            if array.is_null(row) {
                values.push(Value::Null);
                continue;
            }
            let value = match field.data_type() {
                ArrowType::Boolean => Value::Boolean(array.as_any().downcast_ref::<BooleanArray>()
                    .ok_or_else(|| Error::Type("invalid Boolean Arrow array".into()))?.value(row)),
                ArrowType::Int8 => Value::Integer(i64::from(array.as_any().downcast_ref::<Int8Array>()
                    .ok_or_else(|| Error::Type("invalid Int8 Arrow array".into()))?.value(row))),
                ArrowType::Int16 => Value::Integer(i64::from(array.as_any().downcast_ref::<Int16Array>()
                    .ok_or_else(|| Error::Type("invalid Int16 Arrow array".into()))?.value(row))),
                ArrowType::UInt8 => Value::Integer(i64::from(array.as_any().downcast_ref::<UInt8Array>()
                    .ok_or_else(|| Error::Type("invalid UInt8 Arrow array".into()))?.value(row))),
                ArrowType::UInt16 => Value::Integer(i64::from(array.as_any().downcast_ref::<UInt16Array>()
                    .ok_or_else(|| Error::Type("invalid UInt16 Arrow array".into()))?.value(row))),
                ArrowType::UInt32 => Value::Integer(i64::from(array.as_any().downcast_ref::<UInt32Array>()
                    .ok_or_else(|| Error::Type("invalid UInt32 Arrow array".into()))?.value(row))),
                ArrowType::Int32 => Value::Integer(i64::from(array.as_any().downcast_ref::<Int32Array>()
                    .ok_or_else(|| Error::Type("invalid Int32 Arrow array".into()))?.value(row))),
                ArrowType::Int64 => Value::Integer(array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| Error::Type("invalid Int64 Arrow array".into()))?.value(row)),
                ArrowType::UInt64 => Value::Integer(i64::try_from(array.as_any().downcast_ref::<UInt64Array>()
                    .ok_or_else(|| Error::Type("invalid UInt64 Arrow array".into()))?.value(row))
                    .map_err(|_| Error::Type("UInt64 exceeds native integer range".into()))?),
                ArrowType::Float32 => Value::Float(f64::from(array.as_any().downcast_ref::<Float32Array>()
                    .ok_or_else(|| Error::Type("invalid Float32 Arrow array".into()))?.value(row))),
                ArrowType::Float64 => Value::Float(array.as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| Error::Type("invalid Float64 Arrow array".into()))?.value(row)),
                ArrowType::Utf8 => Value::Text(array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| Error::Type("invalid Utf8 Arrow array".into()))?.value(row).into()),
                ArrowType::LargeUtf8 => Value::Text(array.as_any().downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| Error::Type("invalid LargeUtf8 Arrow array".into()))?.value(row).into()),
                _ => unreachable!("validated Arrow type"),
            };
            values.push(value);
        }
        columns.push(values);
    }
    Batch::new(Schema::new(fields)?, columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_nullable_native_batches() {
        let original = Batch::from_rows(
            Schema::new(vec![
                Field::new("name", DataType::Text, true),
                Field::new("n", DataType::Integer, false),
                Field::new("ok", DataType::Boolean, true),
            ]).unwrap(),
            vec![
                vec![Value::Text("a".into()), Value::Integer(10), Value::Boolean(true)],
                vec![Value::Null, Value::Integer(20), Value::Null],
            ],
        ).unwrap();
        assert_eq!(from_arrow(&to_arrow(&original).unwrap()).unwrap(), original);
    }

    #[test]
    fn rejects_out_of_range_unsigned_integers() {
        let array = Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef;
        let record = RecordBatch::try_from_iter(vec![("n", array)]).unwrap();
        assert!(from_arrow(&record).is_err());
    }
}

#[cfg(test)]
mod arrow_narrow_integer_tests {
    use super::*;

    #[test]
    fn decodes_nullable_signed_and_unsigned_small_integers() {
        let record = RecordBatch::try_from_iter(vec![
            ("tiny", Arc::new(Int8Array::from(vec![Some(-7), None])) as ArrayRef),
            ("small", Arc::new(Int16Array::from(vec![Some(-32000), None])) as ArrayRef),
            ("u8", Arc::new(UInt8Array::from(vec![Some(255), None])) as ArrayRef),
            ("u16", Arc::new(UInt16Array::from(vec![Some(65535), None])) as ArrayRef),
            ("u32", Arc::new(UInt32Array::from(vec![Some(u32::MAX), None])) as ArrayRef),
        ]).unwrap();
        let decoded = from_arrow(&record).unwrap();
        assert_eq!(decoded.row(0).unwrap(), vec![
            Value::Integer(-7), Value::Integer(-32000), Value::Integer(255),
            Value::Integer(65535), Value::Integer(i64::from(u32::MAX)),
        ]);
        assert_eq!(decoded.row(1).unwrap(), vec![Value::Null; 5]);
        assert_eq!(from_arrow(&to_arrow(&decoded).unwrap()).unwrap(), decoded);
    }
}

#[cfg(test)]
mod arrow_column_dispatch_perf_tests {
    use super::*;

    #[test]
    fn common_typed_columns_decode_nulls_and_unicode() {
        let record = RecordBatch::try_from_iter(vec![
            ("flag", Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])) as ArrayRef),
            ("id", Arc::new(Int64Array::from(vec![Some(i64::MAX), None, Some(-7)])) as ArrayRef),
            ("score", Arc::new(Float64Array::from(vec![Some(-0.0), None, Some(f64::NAN)])) as ArrayRef),
            ("label", Arc::new(StringArray::from(vec![Some("é🙂"), None, Some("")])) as ArrayRef),
        ]).unwrap();
        let decoded = from_arrow(&record).unwrap();
        assert_eq!(decoded.num_rows(), 3);
        assert_eq!(decoded.value(0, 0).unwrap(), &Value::Boolean(true));
        assert_eq!(decoded.value(1, 1).unwrap(), &Value::Null);
        assert_eq!(decoded.value(0, 2).unwrap(), &Value::Float(-0.0));
        assert!(matches!(decoded.value(2, 2).unwrap(), Value::Float(x) if x.is_nan()));
        assert_eq!(decoded.value(0, 3).unwrap(), &Value::Text("é🙂".into()));
    }
}

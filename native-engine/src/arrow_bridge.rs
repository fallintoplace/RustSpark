//! Convert native batches to and from Apache Arrow without DataFusion.
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, DictionaryArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array,
    LargeStringArray, RecordBatch, RecordBatchOptions, StringArray, UInt64Array,
    builder::StringBuilder, types::{Int8Type, Int16Type, Int32Type, Int64Type,
        UInt8Type, UInt16Type, UInt32Type, UInt64Type},
};
use arrow_schema::{DataType as ArrowType, Field as ArrowField, Schema as ArrowSchema};

use crate::{
    batch::Batch,
    scalar::Value,
    schema::{DataType, Field, Schema},
    Error, Result,
};

const MAX_ARROW_DECODED_TEXT_BYTES: usize = 64 * 1024 * 1024;

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
    // An Arrow record batch without columns still has a logical row count.
    // Supply it explicitly so SELECT literals/zero-column projections survive.
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(Arc::new(ArrowSchema::new(fields)), columns, &options)
        .map_err(|err| Error::Execution(format!("Arrow conversion failed: {err}")))
}


// Decode the dictionary's keys without expanding or copying dictionary values
// per row. Checked conversion and bounds ensure malformed input returns an error.
macro_rules! decode_text_dictionary {
    ($array:expr, $key_type:ty, $budget:expr) => {{
        let dictionary = $array.as_any().downcast_ref::<DictionaryArray<$key_type>>()
            .ok_or_else(|| Error::Type("invalid Arrow dictionary array".into()))?;
        let values = dictionary.values();
        let utf8 = values.as_any().downcast_ref::<StringArray>();
        let large_utf8 = values.as_any().downcast_ref::<LargeStringArray>();
        if utf8.is_none() && large_utf8.is_none() {
            return Err(Error::Type("dictionary values are not UTF-8".into()));
        }
        let mut decoded = Vec::with_capacity(dictionary.len());
        for row in 0..dictionary.len() {
            if dictionary.is_null(row) {
                decoded.push(Value::Null);
                continue;
            }
            let key = usize::try_from(dictionary.keys().value(row))
                .map_err(|_| Error::Type("negative or oversized dictionary key".into()))?;
            if key >= values.len() {
                return Err(Error::Type("Arrow dictionary key out of bounds".into()));
            }
            if values.is_null(key) {
                decoded.push(Value::Null);
            } else if let Some(strings) = utf8 {
                let value = strings.value(key);
                *$budget = $budget.checked_sub(value.len())
                    .ok_or_else(|| Error::Execution("Arrow text decode budget exceeded".into()))?;
                decoded.push(Value::Text(value.into()));
            } else if let Some(strings) = large_utf8 {
                let value = strings.value(key);
                *$budget = $budget.checked_sub(value.len())
                    .ok_or_else(|| Error::Execution("Arrow text decode budget exceeded".into()))?;
                decoded.push(Value::Text(value.into()));
            }
        }
        decoded
    }};
}

pub fn from_arrow(input: &RecordBatch) -> Result<Batch> {
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    // A small Arrow dictionary may expand into gigabytes of native strings.
    // Share the budget across every decoded TEXT column in this record batch.
    let mut remaining_text_bytes = MAX_ARROW_DECODED_TEXT_BYTES;
    for (index, field) in input.schema().fields().iter().enumerate() {
        let array = input.column(index);
        let data_type = match field.data_type() {
            ArrowType::Boolean => DataType::Boolean,
            ArrowType::Int8 | ArrowType::Int16 | ArrowType::Int32 | ArrowType::Int64 |
            ArrowType::UInt8 | ArrowType::UInt16 | ArrowType::UInt32 |
            ArrowType::UInt64 => DataType::Integer,
            ArrowType::Float32 | ArrowType::Float64 => DataType::Float,
            ArrowType::Utf8 | ArrowType::LargeUtf8 => DataType::Text,
            ArrowType::Dictionary(key, value)
                if matches!(key.as_ref(),
                    ArrowType::Int8 | ArrowType::Int16 | ArrowType::Int32 | ArrowType::Int64 |
                    ArrowType::UInt8 | ArrowType::UInt16 | ArrowType::UInt32 | ArrowType::UInt64)
                    && matches!(value.as_ref(), ArrowType::Utf8 | ArrowType::LargeUtf8) =>
                DataType::Text,
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
            ArrowType::Int8 => {
                let values = array.as_any().downcast_ref::<Int8Array>()
                    .ok_or_else(|| Error::Type("invalid Int8 Arrow array".into()))?;
                Some((0..values.len()).map(|row| {
                    if values.is_null(row) { Value::Null }
                    else { Value::Integer(i64::from(values.value(row))) }
                }).collect())
            }
            ArrowType::Int16 => {
                let values = array.as_any().downcast_ref::<Int16Array>()
                    .ok_or_else(|| Error::Type("invalid Int16 Arrow array".into()))?;
                Some((0..values.len()).map(|row| {
                    if values.is_null(row) { Value::Null }
                    else { Value::Integer(i64::from(values.value(row))) }
                }).collect())
            }
            ArrowType::Int32 => {
                let values = array.as_any().downcast_ref::<Int32Array>()
                    .ok_or_else(|| Error::Type("invalid Int32 Arrow array".into()))?;
                Some((0..values.len()).map(|row| {
                    if values.is_null(row) { Value::Null }
                    else { Value::Integer(i64::from(values.value(row))) }
                }).collect())
            }
            ArrowType::UInt8 => {
                let values = array.as_any().downcast_ref::<UInt8Array>()
                    .ok_or_else(|| Error::Type("invalid UInt8 Arrow array".into()))?;
                Some((0..values.len()).map(|row| {
                    if values.is_null(row) { Value::Null }
                    else { Value::Integer(i64::from(values.value(row))) }
                }).collect())
            }
            ArrowType::UInt16 => {
                let values = array.as_any().downcast_ref::<UInt16Array>()
                    .ok_or_else(|| Error::Type("invalid UInt16 Arrow array".into()))?;
                Some((0..values.len()).map(|row| {
                    if values.is_null(row) { Value::Null }
                    else { Value::Integer(i64::from(values.value(row))) }
                }).collect())
            }
            ArrowType::UInt32 => {
                let values = array.as_any().downcast_ref::<UInt32Array>()
                    .ok_or_else(|| Error::Type("invalid UInt32 Arrow array".into()))?;
                Some((0..values.len()).map(|row| {
                    if values.is_null(row) { Value::Null }
                    else { Value::Integer(i64::from(values.value(row))) }
                }).collect())
            }
            ArrowType::UInt64 => {
                let values = array.as_any().downcast_ref::<UInt64Array>()
                    .ok_or_else(|| Error::Type("invalid UInt64 Arrow array".into()))?;
                // The UInt64-to-BIGINT boundary is checked per non-null value,
                // but the Arrow downcast itself is shared by the whole column.
                Some((0..values.len()).map(|row| {
                    if values.is_null(row) {
                        Ok(Value::Null)
                    } else {
                        i64::try_from(values.value(row)).map(Value::Integer)
                            .map_err(|_| Error::Type("UInt64 exceeds native integer range".into()))
                    }
                }).collect::<Result<Vec<_>>>()?)
            }
            ArrowType::Float32 => {
                let values = array.as_any().downcast_ref::<Float32Array>()
                    .ok_or_else(|| Error::Type("invalid Float32 Arrow array".into()))?;
                Some((0..values.len()).map(|row| {
                    if values.is_null(row) { Value::Null }
                    else { Value::Float(f64::from(values.value(row))) }
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
                    if values.is_null(row) { Ok(Value::Null) }
                    else {
                        let text = values.value(row);
                        remaining_text_bytes = remaining_text_bytes.checked_sub(text.len())
                            .ok_or_else(|| Error::Execution("Arrow text decode budget exceeded".into()))?;
                        Ok(Value::Text(text.into()))
                    }
                }).collect::<Result<Vec<_>>>()?)
            }
            ArrowType::LargeUtf8 => {
                let values = array.as_any().downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| Error::Type("invalid LargeUtf8 Arrow array".into()))?;
                Some((0..values.len()).map(|row| {
                    if values.is_null(row) { Ok(Value::Null) }
                    else {
                        let text = values.value(row);
                        remaining_text_bytes = remaining_text_bytes.checked_sub(text.len())
                            .ok_or_else(|| Error::Execution("Arrow text decode budget exceeded".into()))?;
                        Ok(Value::Text(text.into()))
                    }
                }).collect::<Result<Vec<_>>>()?)
            }
            ArrowType::Dictionary(key, value)
                if matches!(value.as_ref(), ArrowType::Utf8 | ArrowType::LargeUtf8) => {
                Some(match key.as_ref() {
                    ArrowType::Int8 => decode_text_dictionary!(array, Int8Type, &mut remaining_text_bytes),
                    ArrowType::Int16 => decode_text_dictionary!(array, Int16Type, &mut remaining_text_bytes),
                    ArrowType::Int32 => decode_text_dictionary!(array, Int32Type, &mut remaining_text_bytes),
                    ArrowType::Int64 => decode_text_dictionary!(array, Int64Type, &mut remaining_text_bytes),
                    ArrowType::UInt8 => decode_text_dictionary!(array, UInt8Type, &mut remaining_text_bytes),
                    ArrowType::UInt16 => decode_text_dictionary!(array, UInt16Type, &mut remaining_text_bytes),
                    ArrowType::UInt32 => decode_text_dictionary!(array, UInt32Type, &mut remaining_text_bytes),
                    ArrowType::UInt64 => decode_text_dictionary!(array, UInt64Type, &mut remaining_text_bytes),
                    _ => return Err(Error::Unsupported("unsupported Arrow dictionary key type".into())),
                })
            }
            _ => None,
        };
        if let Some(values) = common {
            // The Arrow array's physical type guarantees each decoded scalar
            // type. Dictionary value slots can still contain NULL even when
            // the dictionary key itself is present, so check that special
            // case before trusting decoded column values.
            if !field.is_nullable() &&
                (array.null_count() != 0 ||
                    matches!(field.data_type(), ArrowType::Dictionary(_, _)) &&
                    values.iter().any(Value::is_null)) {
                return Err(Error::Type(format!(
                    "NULL in required column {}", field.name()
                )));
            }
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
    // Every decoded scalar was matched against its Arrow field type and
    // requiredness. Skip rechecking each cell in a second full column scan.
    Batch::from_validated_columns(Schema::new(fields)?, columns, input.num_rows())
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

#[cfg(test)]
mod arrow_dictionary_utf8_decode_tests {
    use super::*;
    use arrow_array::builder::StringDictionaryBuilder;

    #[test]
    fn decodes_nullable_dictionary_text_to_native_columns() {
        let mut dictionary = StringDictionaryBuilder::<Int32Type>::new();
        dictionary.append("Berlin").unwrap();
        dictionary.append_null();
        dictionary.append("Berlin").unwrap();
        dictionary.append("東京").unwrap();
        let record = RecordBatch::try_from_iter(vec![
            ("city", Arc::new(dictionary.finish()) as ArrayRef),
        ]).unwrap();
        let native = from_arrow(&record).unwrap();
        assert_eq!(native.schema().fields()[0].data_type, DataType::Text);
        assert_eq!(native.column(0).unwrap(), &[
            Value::Text("Berlin".into()), Value::Null,
            Value::Text("Berlin".into()), Value::Text("東京".into()),
        ]);
        assert_eq!(from_arrow(&to_arrow(&native).unwrap()).unwrap(), native);
    }
}

#[cfg(test)]
mod nullable_arrow_dictionary_value_tests {
    use super::*;

    #[test]
    fn dictionary_value_null_is_not_decoded_as_empty_text() {
        let values = StringArray::from(vec![Some("known"), None]);
        let keys = Int32Array::from(vec![Some(0), Some(1), None]);
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            keys, Arc::new(values),
        ).unwrap();
        let record = RecordBatch::try_from_iter(vec![
            ("city", Arc::new(dictionary) as ArrayRef),
        ]).unwrap();
        let native = from_arrow(&record).unwrap();
        assert_eq!(native.column(0).unwrap(), &[
            Value::Text("known".into()), Value::Null, Value::Null,
        ]);
    }
}

#[cfg(test)]
mod arrow_narrow_batch_dispatch_tests {
    use super::*;

    #[test]
    fn vector_decoding_preserves_narrow_values_and_nulls() {
        let record = RecordBatch::try_from_iter(vec![
            ("i32", Arc::new(Int32Array::from(vec![Some(-123), None, Some(42)])) as ArrayRef),
            ("u32", Arc::new(UInt32Array::from(vec![Some(u32::MAX), None, Some(0)])) as ArrayRef),
            ("f32", Arc::new(Float32Array::from(vec![Some(1.25), None, Some(-0.0)])) as ArrayRef),
        ]).unwrap();
        let batch = from_arrow(&record).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.row(0).unwrap(), vec![
            Value::Integer(-123),
            Value::Integer(i64::from(u32::MAX)),
            Value::Float(1.25),
        ]);
        assert_eq!(batch.row(1).unwrap(), vec![Value::Null; 3]);
        assert_eq!(batch.value(2, 2).unwrap(), &Value::Float(-0.0));
    }
}

#[cfg(test)]
mod zero_column_arrow_cardinality_tests {
    use super::*;

    #[test]
    fn zero_width_native_batches_keep_nonzero_row_counts() {
        let input = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 7).unwrap();
        let record = to_arrow(&input).unwrap();
        assert_eq!(record.num_columns(), 0);
        assert_eq!(record.num_rows(), 7);
        assert_eq!(from_arrow(&record).unwrap(), input);
        let empty = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 0).unwrap();
        assert_eq!(from_arrow(&to_arrow(&empty).unwrap()).unwrap(), empty);
    }

    #[test]
    fn externally_constructed_empty_schema_preserves_cardinality() {
        let arrow = RecordBatch::try_new_with_options(
            Arc::new(ArrowSchema::empty()), vec![],
            &RecordBatchOptions::new().with_row_count(Some(3)),
        ).unwrap();
        let native = from_arrow(&arrow).unwrap();
        assert_eq!(native.num_rows(), 3);
        assert_eq!(native.rows().unwrap(), vec![vec![]; 3]);
    }
}

#[cfg(test)]
mod arrow_unsigned_64_columnar_fast_path_tests {
    use super::*;

    #[test]
    fn converts_large_uint64_columns_without_losing_nulls_or_sign_bounds() {
        let values = (0..8192).map(|row| {
            if row % 97 == 0 { None }
            else { Some(i64::MAX as u64 - row as u64) }
        }).collect::<Vec<_>>();
        let record = RecordBatch::try_from_iter(vec![
            ("n", Arc::new(UInt64Array::from(values.clone())) as ArrayRef)
        ]).unwrap();
        let native = from_arrow(&record).unwrap();
        assert_eq!(native.num_rows(), 8192);
        for (row, original) in values.iter().enumerate() {
            assert_eq!(native.value(row, 0).unwrap(), &original
                .map_or(Value::Null, |value| Value::Integer(value as i64)));
        }
    }

    #[test]
    fn rejects_uint64_overflow_late_in_a_nullable_column() {
        let input = Arc::new(UInt64Array::from(vec![
            None, Some(0), Some(i64::MAX as u64), Some(i64::MAX as u64 + 1)
        ])) as ArrayRef;
        let record = RecordBatch::try_from_iter(vec![("n", input)]).unwrap();
        assert!(matches!(from_arrow(&record), Err(Error::Type(_))));
    }
}

#[cfg(test)]
mod arrow_decoded_column_validation_tests {
    use super::*;

    #[test]
    fn nonnullable_dictionary_rejects_null_dictionary_value_slots() {
        let values = StringArray::from(vec![Some("known"), None]);
        let keys = Int32Array::from(vec![Some(0), Some(1)]);
        let array = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
        let field = ArrowField::new(
            "city",
            ArrowType::Dictionary(Box::new(ArrowType::Int32), Box::new(ArrowType::Utf8)),
            false,
        );
        let record = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![field])),
            vec![Arc::new(array)],
        ).unwrap();
        assert!(from_arrow(&record).unwrap_err().to_string()
            .contains("NULL in required column city"));
    }

    #[test]
    fn trusted_decode_preserves_mixed_nullable_columns() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("amount", DataType::Float, true),
            Field::new("payload", DataType::Text, true),
        ]).unwrap();
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Integer(7), Value::Float(1.25), Value::Text("界🙂".into())],
            vec![Value::Integer(8), Value::Null, Value::Null],
        ]).unwrap();
        assert_eq!(from_arrow(&to_arrow(&batch).unwrap()).unwrap(), batch);
    }
}

#[cfg(test)]
mod varied_dictionary_key_widths {
    use super::*;

    #[test]
    fn decodes_signed_and_unsigned_dictionary_keys_with_large_utf8_values() {
        let names: ArrayRef = Arc::new(StringArray::from(vec![
            Some("Berlin"), None, Some("東京"),
        ]));
        let narrow = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(2), Some(1), None]),
            Arc::clone(&names),
        ).unwrap();
        let wide = DictionaryArray::<UInt16Type>::try_new(
            UInt16Array::from(vec![Some(0), Some(1), None, Some(0)]),
            Arc::new(LargeStringArray::from(vec![
                Some("east"), Some("é🙂"),
            ])),
        ).unwrap();
        let arrow = RecordBatch::try_from_iter(vec![
            ("small", Arc::new(narrow) as ArrayRef),
            ("large", Arc::new(wide) as ArrayRef),
        ]).unwrap();
        let native = from_arrow(&arrow).unwrap();
        assert_eq!(native.num_rows(), 4);
        assert_eq!(native.rows().unwrap(), vec![
            vec![Value::Text("Berlin".into()), Value::Text("east".into())],
            vec![Value::Text("東京".into()), Value::Text("é🙂".into())],
            vec![Value::Null, Value::Null],
            vec![Value::Null, Value::Text("east".into())],
        ]);
        assert_eq!(from_arrow(&to_arrow(&native).unwrap()).unwrap(), native);
    }

    #[test]
    fn decodes_unsigned_32_bit_keys_without_coercion() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("first"), Some("last"),
        ]));
        let dictionary = DictionaryArray::<UInt32Type>::try_new(
            UInt32Array::from(vec![Some(1), Some(0), Some(1)]), values,
        ).unwrap();
        let arrow = RecordBatch::try_from_iter(vec![
            ("label", Arc::new(dictionary) as ArrayRef),
        ]).unwrap();
        let native = from_arrow(&arrow).unwrap();
        assert_eq!(native.column(0).unwrap(), &[
            Value::Text("last".into()),
            Value::Text("first".into()),
            Value::Text("last".into()),
        ]);
    }
}

#[cfg(test)]
mod arrow_decoded_text_budget_regressions {
    use super::*;

    #[test]
    fn dictionary_expansion_is_bounded_before_allocating_many_copies() {
        let value = "z".repeat(1_048_576);
        let source = StringArray::from(vec![value.as_str()]);
        let keys = Int32Array::from(vec![0; 65]);
        let dictionary = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(source)).unwrap();
        let arrow = RecordBatch::try_from_iter(vec![
            ("label", Arc::new(dictionary) as ArrayRef),
        ]).unwrap();
        assert!(from_arrow(&arrow).unwrap_err().to_string().contains("decode budget"));
    }

    #[test]
    fn regular_text_columns_share_the_same_decode_budget() {
        let value = "x".repeat(34 * 1024 * 1024);
        let values = Arc::new(StringArray::from(vec![value.as_str()])) as ArrayRef;
        let arrow = RecordBatch::try_from_iter(vec![
            ("left", values.clone()), ("right", values),
        ]).unwrap();
        assert!(from_arrow(&arrow).unwrap_err().to_string().contains("decode budget"));
    }
}

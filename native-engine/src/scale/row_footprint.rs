//! Common retained-memory estimate for streaming row collections.
use crate::scalar::Value;

fn measure<'a>(
    values: impl ExactSizeIterator<Item = &'a Value>,
    text_bytes: impl Fn(&String) -> usize,
) -> usize {
    let initial = 64usize.saturating_add(values.len().saturating_mul(std::mem::size_of::<Value>()));
    values.fold(initial, |total, value| match value {
        Value::Text(text) => total.saturating_add(text_bytes(text)),
        _ => total,
    })
}

pub(super) fn footprint<'a>(values: impl ExactSizeIterator<Item = &'a Value>) -> usize {
    measure(values, String::capacity)
}

// Retained memory is constant when the schema cannot contain TEXT.
pub(super) fn fixed_footprint(schema: &crate::schema::Schema) -> Option<usize> {
    if schema.fields().iter().any(|field| field.data_type == crate::schema::DataType::Text) {
        return None;
    }
    Some(64usize.saturating_add(
        schema.len().saturating_mul(std::mem::size_of::<Value>()),
    ))
}

// Lower bound for the memory needed to clone a row. The input STRING
// capacity is not the capacity of the separate retained clone.
pub(super) fn minimum_cloned_footprint<'a>(
    values: impl ExactSizeIterator<Item = &'a Value>,
) -> usize {
    measure(values, String::len)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accounts_for_retained_text_capacity_not_just_length() {
        let mut text = String::with_capacity(8192);
        text.push('x');
        let capacity = text.capacity();
        let values = [Value::Text(text), Value::Null, Value::Integer(7)];
        let expected = 64 + values.len() * std::mem::size_of::<Value>() + capacity;
        assert_eq!(footprint(values.iter()), expected);
        assert!(footprint(values.iter()) > 64 + values.len() * std::mem::size_of::<Value>() + 1);
        assert_eq!(minimum_cloned_footprint(values.iter()),
            64 + values.len() * std::mem::size_of::<Value>() + 1);
    }
}

#[cfg(test)]
mod fixed_schema_footprint_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn nullable_numeric_rows_have_constant_retained_size() {
        let schema = Schema::new(vec![
            Field::new("i", DataType::Integer, true),
            Field::new("f", DataType::Float, true),
            Field::new("b", DataType::Boolean, true),
        ]).unwrap();
        let expected = fixed_footprint(&schema).unwrap();
        for values in [
            [Value::Null, Value::Null, Value::Null],
            [Value::Integer(1), Value::Float(2.0), Value::Boolean(true)],
        ] {
            assert_eq!(footprint(values.iter()), expected);
        }
        let text = Schema::new(vec![Field::new("t", DataType::Text, false)]).unwrap();
        assert_eq!(fixed_footprint(&text), None);
        assert_eq!(fixed_footprint(&Schema::new(vec![]).unwrap()), Some(64));
    }
}

//! Common retained-memory estimate for streaming row collections.
use crate::scalar::Value;

pub(super) fn footprint<'a>(values: impl ExactSizeIterator<Item = &'a Value>) -> usize {
    let initial = 64usize.saturating_add(values.len().saturating_mul(std::mem::size_of::<Value>()));
    values.fold(initial, |total, value| match value {
        Value::Text(text) => total.saturating_add(text.capacity()),
        _ => total,
    })
}

// Lower bound for the memory needed to clone a row. The input STRING
// capacity is not the capacity of the separate retained clone.
pub(super) fn minimum_cloned_footprint<'a>(
    values: impl ExactSizeIterator<Item = &'a Value>,
) -> usize {
    let initial = 64usize.saturating_add(values.len().saturating_mul(std::mem::size_of::<Value>()));
    values.fold(initial, |total, value| match value {
        Value::Text(text) => total.saturating_add(text.len()),
        _ => total,
    })
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

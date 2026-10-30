//! Common retained-memory estimate for streaming row collections.
use crate::scalar::Value;

pub(super) fn footprint<'a>(values: impl ExactSizeIterator<Item = &'a Value>) -> usize {
    let initial = 64usize.saturating_add(values.len().saturating_mul(std::mem::size_of::<Value>()));
    values.fold(initial, |total, value| match value {
        Value::Text(text) => total.saturating_add(text.len()),
        _ => total,
    })
}

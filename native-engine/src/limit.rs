use crate::{batch::Batch, Error, Result};

fn validate_window(offset: usize, count: Option<usize>) -> Result<()> {
    if let Some(count) = count {
        offset.checked_add(count)
            .ok_or_else(|| Error::Execution("LIMIT + OFFSET overflow".into()))?;
    }
    Ok(())
}

pub fn limit(batch: &Batch, offset: usize, count: Option<usize>) -> Result<Batch> {
    validate_window(offset, count)?;
    let start = offset.min(batch.num_rows());
    let end = count.map_or(batch.num_rows(), |count| {
        start.saturating_add(count).min(batch.num_rows())
    });
    if start == 0 && end == batch.num_rows() {
        return Ok(batch.clone());
    }
    batch.slice_rows(start, end)
}

/// Apply LIMIT to an owned native batch, avoiding a deep copy when
/// the requested window covers all its rows.
pub fn limit_owned(batch: Batch, offset: usize, count: Option<usize>) -> Result<Batch> {
    validate_window(offset, count)?;
    if offset == 0 && count.is_none_or(|count| count >= batch.num_rows()) {
        return Ok(batch);
    }
    let start = offset.min(batch.num_rows());
    let end = count.map_or(batch.num_rows(), |count| {
        start.saturating_add(count).min(batch.num_rows())
    });
    batch.slice_rows_owned(start, end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        scalar::Value,
        schema::{DataType, Field, Schema},
    };

    #[test]
    fn pagination_is_bounded() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("x", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]],
        )
        .unwrap();
        assert_eq!(limit(&batch, 1, Some(1)).unwrap().rows().unwrap(), vec![vec![Value::Integer(2)]]);
        assert_eq!(limit(&batch, 9, Some(2)).unwrap().num_rows(), 0);
        assert!(limit(&batch, usize::MAX, Some(1)).is_err());
    }
}

#[cfg(test)]
mod direct_limit_slice_tests {
    use super::*;
    use crate::scalar::Value;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn huge_offset_returns_empty_and_keeps_schema() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let input = Batch::from_rows(schema.clone(), vec![vec![Value::Integer(1)]]).unwrap();
        let result = limit(&input, usize::MAX, None).unwrap();
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.schema(), &schema);
    }
}

#[cfg(test)]
mod consolidated_full_range_limit_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    #[test]
    fn full_limit_returns_equivalent_batch_and_keeps_empty_schema_rows() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema, vec![vec![Value::Integer(1)]]).unwrap();
        assert_eq!(limit(&batch, 0, None).unwrap(), batch);
        assert_eq!(limit(&batch, 0, Some(9)).unwrap(), batch);
    }
}

#[cfg(test)]
mod native_owned_limit_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn full_range_moves_columns_and_partial_range_matches_old_limit() {
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("name", DataType::Text, true),
        ]).unwrap(), vec![
            vec![Value::Text("large".repeat(512))], vec![Value::Null],
            vec![Value::Text("last".into())],
        ]).unwrap();
        for window in [(0, None), (0, Some(30)), (1, Some(2)), (10, None)] {
            assert_eq!(limit_owned(batch.clone(), window.0, window.1).unwrap(),
                limit(&batch, window.0, window.1).unwrap());
        }
        assert!(limit_owned(batch, usize::MAX, Some(1)).is_err());
    }
}

#[cfg(test)]
mod move_owned_limit_payload_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn selecting_partial_window_moves_original_text_payload() {
        let schema = Schema::new(vec![Field::new("payload", DataType::Text, true)]).unwrap();
        let input = Batch::from_rows(schema, vec![
            vec![Value::Null],
            vec![Value::Text("payload".repeat(2048))],
            vec![Value::Text("tail".into())],
        ]).unwrap();
        let ptr = match input.value(1, 0).unwrap() {
            Value::Text(text) => text.as_ptr(),
            _ => unreachable!(),
        };
        let limited = limit_owned(input, 1, Some(1)).unwrap();
        assert_eq!(limited.num_rows(), 1);
        assert_eq!(match limited.value(0, 0).unwrap() {
            Value::Text(text) => text.as_ptr(),
            _ => unreachable!(),
        }, ptr);
    }
}

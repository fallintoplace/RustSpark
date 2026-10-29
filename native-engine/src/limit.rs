use crate::{batch::Batch, Error, Result};

pub fn limit(batch: &Batch, offset: usize, count: Option<usize>) -> Result<Batch> {
    if let Some(count) = count {
        offset
            .checked_add(count)
            .ok_or_else(|| Error::Execution("LIMIT + OFFSET overflow".into()))?;
    }
    let start = offset.min(batch.num_rows());
    let end = count.map_or(batch.num_rows(), |count| {
        start.saturating_add(count).min(batch.num_rows())
    });
    if start == 0 && end == batch.num_rows() {
        return Ok(batch.clone());
    }
    batch.slice_rows(start, end)
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

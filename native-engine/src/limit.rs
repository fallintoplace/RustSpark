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
    #[test]
    fn unbounded_limit_preserves_the_original_batch() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]],
        ).unwrap();
        assert_eq!(limit(&input, 0, None).unwrap(), input);
        assert_eq!(limit(&input, 0, Some(10)).unwrap(), input);
        assert_eq!(limit(&input, 1, Some(1)).unwrap().num_rows(), 1);
    }
}

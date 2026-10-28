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
    batch.take(&(start..end).collect::<Vec<_>>())
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

//! Bounded top-K selection for ORDER BY with LIMIT and OFFSET.
//! The requested K is limited, and each batch is reduced before merging.
use crate::{
    batch::Batch, batch_chunks::{self, OwnedBatchChunks}, batch_utils,
    limit, schema::Schema, sort::SortKey, topk, Error, Result,
};
use super::cancel::QueryControl;

pub const MAX_TOPK_ROWS: usize = 8_192;

/// Keep at most K candidates across arbitrary input batches.
/// On fallback, the caller retains the existing external-sort pipeline.
pub fn top_k_stream<I>(
    input: I, schema: Schema, keys: &[SortKey],
    offset: usize, count: usize, batch_rows: usize,
    control: &QueryControl,
) -> Result<OwnedBatchChunks>
where
    I: IntoIterator<Item = Result<Batch>>,
{
    let k = offset.checked_add(count)
        .filter(|k| *k <= MAX_TOPK_ROWS)
        .ok_or_else(|| Error::Execution("stream Top-K row budget exceeded".into()))?;
    if batch_rows == 0 {
        return Err(Error::Execution("stream Top-K batch size must be positive".into()));
    }
    for key in keys {
        key.expression.validate_budget()?;
        key.expression.output_type(&schema)?;
    }
    control.check()?;
    if k == 0 {
        return batch_chunks::chunks_owned(Batch::empty(schema), batch_rows);
    }
    let mut best: Option<Batch> = None;
    for incoming in input {
        control.check()?;
        let incoming = incoming?;
        if incoming.schema() != &schema {
            return Err(Error::Schema("stream Top-K input schema changed".into()));
        }
        if incoming.num_rows() == 0 { continue; }
        let candidate = topk::top_k_owned(incoming, keys, k)?;
        best = Some(match best.take() {
            None => candidate,
            Some(previous) => {
                let merged = batch_utils::concat(
                    &schema, [previous, candidate], k.saturating_mul(2),
                )?;
                topk::top_k_owned(merged, keys, k)?
            }
        });
    }
    control.check()?;
    let sorted = best.unwrap_or_else(|| Batch::empty(schema));
    let window = limit::limit_owned(sorted, offset, Some(count))?;
    batch_chunks::chunks_owned(window, batch_rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{expr::{Expr, BinaryOp}, scalar::Value,
        schema::{DataType, Field}, sort::SortKey};

    #[test]
    fn topk_handles_offset_ties_and_computed_keys_across_batches() {
        let schema = Schema::new(vec![
            Field::new("v", DataType::Integer, false),
            Field::new("label", DataType::Text, false),
        ]).unwrap();
        let first = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(5), Value::Text("a".into())],
            vec![Value::Integer(1), Value::Text("b".into())],
        ]).unwrap();
        let second = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(3), Value::Text("c".into())],
            vec![Value::Integer(1), Value::Text("d".into())],
            vec![Value::Integer(2), Value::Text("e".into())],
        ]).unwrap();
        let key = SortKey::ascending(Expr::binary(
            Expr::Column("v".into()), BinaryOp::Multiply,
            Expr::Literal(Value::Integer(-1)),
        ));
        let result = top_k_stream(
            [Ok(first), Ok(second)], schema, &[key],
            1, 3, 2, &QueryControl::default(),
        ).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert!(result.iter().all(|batch| batch.num_rows() <= 2));
        let rows = result.iter().flat_map(|batch| batch.rows().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![
            vec![Value::Integer(3), Value::Text("c".into())],
            vec![Value::Integer(2), Value::Text("e".into())],
            vec![Value::Integer(1), Value::Text("b".into())],
        ]);
    }

    #[test]
    fn rejects_overflow_and_invalid_expressions_even_for_zero_rows() {
        let schema = Schema::new(vec![Field::new("v", DataType::Integer, true)]).unwrap();
        let bad = [SortKey::ascending(Expr::Column("missing".into()))];
        assert!(top_k_stream(Vec::<Result<Batch>>::new(), schema.clone(), &bad,
            0, 0, 2, &QueryControl::default()).is_err());
        assert!(top_k_stream(Vec::<Result<Batch>>::new(), schema, &[],
            usize::MAX, 1, 2, &QueryControl::default()).is_err());
    }
}

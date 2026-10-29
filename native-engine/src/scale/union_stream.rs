//! Streaming UNION ALL preserves input order and checks every batch schema.
use crate::{batch::Batch, schema::Schema, Error, Result};
use super::cancel::QueryControl;

pub struct UnionAllStream<L, R> {
    left: L,
    right: R,
    schema: Schema,
    control: QueryControl,
    on_right: bool,
    stopped: bool,
}

impl<L, R> UnionAllStream<L, R> {
    pub fn new(left: L, right: R, schema: Schema, control: QueryControl) -> Self {
        Self { left, right, schema, control, on_right: false, stopped: false }
    }
}

impl<L, R> Iterator for UnionAllStream<L, R>
where L: Iterator<Item = Result<Batch>>, R: Iterator<Item = Result<Batch>>
{
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped { return None; }
        loop {
            if let Err(error) = self.control.check() {
                self.stopped = true;
                return Some(Err(error));
            }
            let next = if self.on_right { self.right.next() } else { self.left.next() };
            let Some(next) = next else {
                if !self.on_right {
                    self.on_right = true;
                    continue;
                }
                self.stopped = true;
                return None;
            };
            match next {
                Ok(batch) if batch.schema() == &self.schema => return Some(Ok(batch)),
                Ok(batch) => {
                    // SQL UNION ALL uses column positions, not names. The output
                    // adopts left input names and widened nullability.
                    if batch.schema().len() != self.schema.len() ||
                        !batch.schema().fields().iter().zip(self.schema.fields())
                            .all(|(source, target)| source.data_type == target.data_type) {
                        self.stopped = true;
                        return Some(Err(Error::Schema("UNION ALL input schemas differ".into())));
                    }
                    let rebased = (0..self.schema.len())
                        .map(|index| batch.column(index).map(|values| values.to_vec()))
                        .collect::<Result<Vec<_>>>()
                        .and_then(|columns| Batch::new(self.schema.clone(), columns));
                    match rebased {
                        Ok(batch) => return Some(Ok(batch)),
                        Err(error) => {
                            self.stopped = true;
                            return Some(Err(error));
                        }
                    }
                }
                Err(error) => {
                    self.stopped = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};

    #[test]
    fn concatenation_stays_bounded_and_ordered() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let one = Batch::from_rows(schema.clone(), vec![vec![Value::Integer(1)]]).unwrap();
        let two = Batch::from_rows(schema.clone(), vec![vec![Value::Integer(2)]]).unwrap();
        let merged = UnionAllStream::new(
            [Ok(one)].into_iter(), [Ok(two)].into_iter(), schema,
            QueryControl::default());
        let rows = merged.collect::<Result<Vec<_>>>().unwrap().iter()
            .flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
    }

    #[test]
    fn rejects_type_mismatches_without_reading_more_input() {
        let left_schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let right_schema = Schema::new(vec![Field::new("n", DataType::Text, false)]).unwrap();
        let right = Batch::from_rows(right_schema, vec![vec![Value::Text("bad".into())]]).unwrap();
        let mut result = UnionAllStream::new(
            std::iter::empty(), [Ok(right)].into_iter(), left_schema,
            QueryControl::default());
        assert!(result.next().unwrap().is_err());
        assert!(result.next().is_none());
    }
}


/// Bounded UNION DISTINCT across two input streams, preserving first appearance.
pub struct UnionDistinctStream<L, R> {
    inner: UnionAllStream<L, R>,
    seen: std::collections::HashSet<Vec<crate::scalar::Value>>,
    retained_bytes: usize,
    max_keys: usize,
    max_bytes: usize,
    stopped: bool,
}

impl<L, R> UnionDistinctStream<L, R> {
    pub fn new(
        left: L, right: R, schema: Schema, control: QueryControl,
        max_keys: usize, max_bytes: usize,
    ) -> Result<Self> {
        if max_keys == 0 || max_bytes == 0 {
            return Err(Error::Execution("UNION DISTINCT requires positive memory limits".into()));
        }
        Ok(Self {
            inner: UnionAllStream::new(left, right, schema, control),
            seen: std::collections::HashSet::new(),
            retained_bytes: 0, max_keys, max_bytes, stopped: false,
        })
    }
}

impl<L, R> Iterator for UnionDistinctStream<L, R>
where L: Iterator<Item = Result<Batch>>, R: Iterator<Item = Result<Batch>>
{
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped { return None; }
        loop {
            let batch = match self.inner.next()? {
                Ok(batch) => batch,
                Err(error) => {
                    self.stopped = true;
                    return Some(Err(error));
                }
            };
            let mut selected = Vec::new();
            for row in 0..batch.num_rows() {
                let key = match batch.row(row) {
                    Ok(key) => key,
                    Err(error) => {
                        self.stopped = true;
                        return Some(Err(error));
                    }
                };
                if self.seen.contains(&key) { continue; }
                let footprint = key.iter().fold(
                    64usize.saturating_add(key.len().saturating_mul(
                        std::mem::size_of::<crate::scalar::Value>())),
                    |total, value| match value {
                        crate::scalar::Value::Text(text) => total.saturating_add(text.len()),
                        _ => total,
                    }
                );
                let next_total = self.retained_bytes.checked_add(footprint);
                if self.seen.len() >= self.max_keys ||
                    next_total.is_none_or(|bytes| bytes > self.max_bytes) {
                    self.stopped = true;
                    return Some(Err(Error::Execution("UNION DISTINCT memory budget exceeded".into())));
                }
                self.retained_bytes = next_total.expect("validated set memory");
                self.seen.insert(key);
                selected.push(row);
            }
            if !selected.is_empty() {
                return Some(batch.take(&selected));
            }
        }
    }
}

#[cfg(test)]
mod union_distinct_stream_feature_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};

    #[test]
    fn stream_set_union_preserves_order_and_deduplicates_nulls() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let a = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Null]
        ]).unwrap();
        let b = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Null]
        ]).unwrap();
        let union = UnionDistinctStream::new(
            [Ok(a)].into_iter(), [Ok(b)].into_iter(), schema,
            QueryControl::default(), 10, 4096
        ).unwrap();
        let rows = union.collect::<Result<Vec<_>>>().unwrap()
            .iter().flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(rows, vec![
            vec![Value::Integer(1)], vec![Value::Null], vec![Value::Integer(2)]
        ]);
    }

    #[test]
    fn exceeding_key_budget_is_a_terminal_error() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)]
        ]).unwrap();
        let mut union = UnionDistinctStream::new(
            [Ok(batch)].into_iter(), std::iter::empty(), schema,
            QueryControl::default(), 1, 4096
        ).unwrap();
        assert!(union.next().unwrap().is_err());
        assert!(union.next().is_none());
    }
}


#[cfg(test)]
mod union_positional_schema_engine_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};

    #[test]
    fn renames_right_columns_to_the_union_output_schema() {
        let a = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let b = Schema::new(vec![Field::new("alias", DataType::Integer, true)]).unwrap();
        let output_schema = Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap();
        let first = Batch::from_rows(a, vec![vec![Value::Integer(1)]]).unwrap();
        let second = Batch::from_rows(b, vec![vec![Value::Null]]).unwrap();
        let results = UnionAllStream::new(
            [Ok(first)].into_iter(), [Ok(second)].into_iter(),
            output_schema.clone(), QueryControl::default(),
        ).collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|part| part.schema() == &output_schema));
        assert_eq!(results[0].row(0).unwrap(), vec![Value::Integer(1)]);
        assert_eq!(results[1].row(0).unwrap(), vec![Value::Null]);
    }
}

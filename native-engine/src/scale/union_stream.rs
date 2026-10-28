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
                Ok(_) => {
                    self.stopped = true;
                    return Some(Err(Error::Schema("UNION ALL input schemas differ".into())));
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

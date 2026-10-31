//! Native streaming Cartesian product with bounded right-side materialization.
use crate::{
    batch::Batch, join, scalar::Value, schema::Schema, Error, Result,
};
use super::{cancel::QueryControl, stream_execute::BatchStream};

const MAX_BUILD_ROWS: usize = 100_000;
const MAX_BUILD_BYTES: usize = 64 * 1024 * 1024;
const MAX_OUTPUT_BATCH_BYTES: usize = 16 * 1024 * 1024;

pub struct CrossJoinStream {
    left: BatchStream,
    left_schema: Schema,
    output_schema: Schema,
    right_rows: Vec<Vec<Value>>,
    right_bytes: Vec<usize>,
    left_batch: Option<Batch>,
    left_row: usize,
    current_left: Option<Vec<Value>>,
    current_left_bytes: usize,
    right_row: usize,
    batch_rows: usize,
    control: QueryControl,
    stopped: bool,
}

impl CrossJoinStream {
    pub fn new(
        left: BatchStream, right: BatchStream, left_schema: Schema,
        right_schema: Schema, batch_rows: usize, control: QueryControl,
    ) -> Result<Self> {
        if batch_rows == 0 || batch_rows > 65_536 {
            return Err(Error::Execution("invalid cross join batch size".into()));
        }
        let output_schema = join::cross_join(
            &Batch::empty(left_schema.clone()), &Batch::empty(right_schema.clone())
        )?.schema().clone();
        let mut right_rows = Vec::new();
        let mut right_bytes = Vec::new();
        let mut size = 0usize;
        for next in right {
            control.check()?;
            let batch = next?;
            if batch.schema() != &right_schema {
                return Err(Error::Schema("stream cross join right schema changed".into()));
            }
            for row in 0..batch.num_rows() {
                if row % 1024 == 0 { control.check()?; }
                if right_rows.len() >= MAX_BUILD_ROWS {
                    return Err(Error::Execution("stream cross join build row budget exceeded".into()));
                }
                let values = batch.row(row)?;
                let footprint = super::row_footprint::footprint(values.iter());
                size = size.checked_add(footprint)
                    .filter(|size| *size <= MAX_BUILD_BYTES)
                    .ok_or_else(|| Error::Execution("stream cross join build byte budget exceeded".into()))?;
                right_rows.push(values);
                right_bytes.push(footprint);
            }
        }
        Ok(Self {
            left, left_schema, output_schema, right_rows, right_bytes,
            left_batch: None, left_row: 0, current_left: None,
            current_left_bytes: 0, right_row: 0,
            batch_rows, control, stopped: false,
        })
    }

    fn produce(&mut self) -> Result<Option<Batch>> {
        if self.right_rows.is_empty() { return Ok(None); }
        let mut output = Vec::with_capacity(self.batch_rows.min(4096));
        let mut output_bytes = 0usize;
        while output.len() < self.batch_rows {
            if output.len() % 256 == 0 { self.control.check()?; }
            if self.current_left.is_none() {
                loop {
                    if let Some(batch) = &self.left_batch {
                        if self.left_row < batch.num_rows() {
                            let row = batch.row(self.left_row)?;
                            self.current_left_bytes = super::row_footprint::footprint(row.iter());
                            self.current_left = Some(row);
                            self.right_row = 0;
                            break;
                        }
                    }
                    let Some(next) = self.left.next() else {
                        if output.is_empty() { return Ok(None); }
                        return Ok(Some(Batch::from_rows(self.output_schema.clone(), output)?));
                    };
                    let batch = next?;
                    if batch.schema() != &self.left_schema {
                        return Err(Error::Schema("stream cross join left schema changed".into()));
                    }
                    self.left_row = 0;
                    self.left_batch = Some(batch);
                }
            }
            let left = self.current_left.as_ref().expect("initialized left row");
            let next_bytes = self.current_left_bytes
                .saturating_add(self.right_bytes[self.right_row]);
            if next_bytes > MAX_OUTPUT_BATCH_BYTES {
                return Err(Error::Execution("stream cross join output row byte budget exceeded".into()));
            }
            if !output.is_empty() && output_bytes.saturating_add(next_bytes) > MAX_OUTPUT_BATCH_BYTES {
                break;
            }
            output_bytes += next_bytes;
            let mut row = left.clone();
            row.extend_from_slice(&self.right_rows[self.right_row]);
            output.push(row);
            self.right_row += 1;
            if self.right_row == self.right_rows.len() {
                self.current_left = None;
                self.left_row += 1;
            }
        }
        Ok(Some(Batch::from_rows(self.output_schema.clone(), output)?))
    }
}

impl Iterator for CrossJoinStream {
    type Item = Result<Batch>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped { return None; }
        match self.produce() {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => { self.stopped = true; None },
            Err(error) => { self.stopped = true; Some(Err(error)) },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};

    fn schema() -> Schema {
        Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap()
    }

    #[test]
    fn streams_product_across_small_output_batches_without_losing_rows() {
        let left = Batch::from_rows(schema(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)],
        ]).unwrap();
        let right = Batch::from_rows(schema(), vec![
            vec![Value::Integer(10)], vec![Value::Null], vec![Value::Integer(30)]
        ]).unwrap();
        let expected = join::cross_join(&left, &right).unwrap().rows().unwrap();
        let batches = CrossJoinStream::new(
            Box::new([Ok(left)].into_iter()), Box::new([Ok(right)].into_iter()),
            schema(), schema(), 2, QueryControl::default(),
        ).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches.iter().map(Batch::num_rows).collect::<Vec<_>>(), vec![2, 2, 2]);
        assert_eq!(batches.iter().flat_map(|batch| batch.rows().unwrap())
            .collect::<Vec<_>>(), expected);
        assert_eq!(batches[0].schema().fields()[1].name, "right_id");
    }

    #[test]
    fn empty_right_side_never_reads_left_input() {
        let right = Batch::empty(schema());
        let stream = CrossJoinStream::new(
            Box::new([Err(Error::Execution("left should not be consumed".into()))].into_iter()),
            Box::new([Ok(right)].into_iter()), schema(), schema(),
            2, QueryControl::default(),
        ).unwrap();
        assert_eq!(stream.collect::<Result<Vec<_>>>().unwrap().len(), 0);
    }
}

#[cfg(test)]
mod cross_join_cached_left_footprint_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn repeats_large_unicode_rows_without_losing_memory_accounting() {
        let schema = Schema::new(vec![Field::new("payload", DataType::Text, false)]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("🙂".repeat(2048))],
            vec![Value::Text("é".repeat(1024))],
        ]).unwrap();
        let right = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("a".into())],
            vec![Value::Text("b".into())],
        ]).unwrap();
        let expected = join::cross_join(&left, &right).unwrap().rows().unwrap();
        let actual = CrossJoinStream::new(Box::new([Ok(left)].into_iter()),
            Box::new([Ok(right)].into_iter()), schema.clone(), schema,
            3, QueryControl::default()).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(actual.iter().flat_map(|batch| batch.rows().unwrap())
            .collect::<Vec<_>>(), expected);
    }
}

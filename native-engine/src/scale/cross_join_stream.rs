//! Native streaming Cartesian product with bounded right-side materialization.
use crate::{
    batch::Batch, join, scalar::Value, schema::Schema, Error, Result,
};
use super::{cancel::QueryControl, stream_execute::BatchStream};

const MAX_BUILD_ROWS: usize = 100_000;
const MAX_BUILD_BYTES: usize = 64 * 1024 * 1024;
const MAX_OUTPUT_BATCH_BYTES: usize = 16 * 1024 * 1024;

// A cloned output row retains at least its TEXT lengths, not the source
// String capacities. Use this lower bound to reject oversized rows before
// cloning and to avoid charging unused capacity in the left input.
fn cloned_row_lower_bound(columns: &[&[Value]], row: usize) -> usize {
    let fixed = 64usize.saturating_add(
        columns.len().saturating_mul(std::mem::size_of::<Value>()),
    );
    columns.iter().fold(fixed, |total, column| match &column[row] {
        Value::Text(value) => total.saturating_add(value.len()),
        _ => total,
    })
}

pub struct CrossJoinStream {
    left: BatchStream,
    left_schema: Schema,
    output_schema: Schema,
    right_rows: Vec<Vec<Value>>,
    right_bytes: Vec<usize>,
    left_batch: Option<Batch>,
    left_row: usize,
    left_ready: bool,
    current_left_bytes: usize,
    right_row: usize,
    batch_rows: usize,
    control: QueryControl,
    pending_error: Option<Error>,
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
            let source_columns = (0..right_schema.len())
                .map(|index| batch.column(index)).collect::<Result<Vec<_>>>()?;
            for row in 0..batch.num_rows() {
                if row % 1024 == 0 { control.check()?; }
                if right_rows.len() >= MAX_BUILD_ROWS {
                    return Err(Error::Execution("stream cross join build row budget exceeded".into()));
                }
                // Do not clone a potentially enormous right row if even its
                // minimum retained size exceeds the build memory budget.
                let minimum = cloned_row_lower_bound(&source_columns, row);
                if size.checked_add(minimum).is_none_or(|next| next > MAX_BUILD_BYTES) {
                    return Err(Error::Execution("stream cross join build byte budget exceeded".into()));
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
            left_batch: None, left_row: 0, left_ready: false,
            current_left_bytes: 0, right_row: 0,
            batch_rows, control, pending_error: None, stopped: false,
        })
    }

    // Emit an already-computed prefix before reporting a later terminal failure.
    fn defer_error(
        &mut self, error: Error, output: Vec<Vec<Value>>, rows: usize,
    ) -> Result<Option<Batch>> {
        if rows == 0 { return Err(error); }
        let batch = Batch::new_with_row_count(self.output_schema.clone(), output, rows)?;
        self.pending_error = Some(error);
        Ok(Some(batch))
    }

    fn produce(&mut self) -> Result<Option<Batch>> {
        if self.right_rows.is_empty() { return Ok(None); }
        // Build column-major output once rather than allocating a Vec per row
        // and transposing it again in Batch::from_rows.
        let width = self.output_schema.len();
        let capacity = self.batch_rows.min(256).min(
            (MAX_OUTPUT_BATCH_BYTES / std::mem::size_of::<Value>().max(1) / width.max(1)).max(1)
        );
        let mut output: Vec<Vec<Value>> = (0..width)
            .map(|_| Vec::with_capacity(capacity)).collect();
        let mut output_rows = 0usize;
        let mut output_bytes = 0usize;
        while output_rows < self.batch_rows {
            if output_rows % 256 == 0 {
                if let Err(error) = self.control.check() {
                    return self.defer_error(error, output, output_rows);
                }
            }
            if !self.left_ready {
                loop {
                    if let Some(batch) = &self.left_batch {
                        if self.left_row < batch.num_rows() {
                            // Retain only the row index. Borrow the left values
                            // from the resident batch until every right row has
                            // been emitted, instead of cloning a wide left row.
                            let columns = (0..self.left_schema.len())
                                .map(|index| batch.column(index))
                                .collect::<Result<Vec<_>>>()?;
                            self.current_left_bytes =
                                cloned_row_lower_bound(&columns, self.left_row);
                            self.left_ready = true;
                            self.right_row = 0;
                            break;
                        }
                    }
                    let Some(next) = self.left.next() else {
                        if output_rows == 0 { return Ok(None); }
                        return Ok(Some(Batch::from_validated_columns(
                            self.output_schema.clone(), output, output_rows,
                        )?));
                    };
                    let batch = match next {
                        Ok(batch) => batch,
                        Err(error) => return self.defer_error(error, output, output_rows),
                    };
                    if batch.schema() != &self.left_schema {
                        return self.defer_error(
                            Error::Schema("stream cross join left schema changed".into()),
                            output, output_rows,
                        );
                    }
                    self.left_row = 0;
                    self.left_batch = Some(batch);
                }
            }
            let left = self.left_batch.as_ref().expect("initialized left batch");
            let next_bytes = self.current_left_bytes
                .saturating_add(self.right_bytes[self.right_row]);
            if next_bytes > MAX_OUTPUT_BATCH_BYTES {
                return self.defer_error(
                    Error::Execution("stream cross join output row byte budget exceeded".into()),
                    output, output_rows,
                );
            }
            if output_rows != 0 && output_bytes.saturating_add(next_bytes) > MAX_OUTPUT_BATCH_BYTES {
                break;
            }
            output_bytes += next_bytes;
            for (index, column) in output.iter_mut().take(self.left_schema.len()).enumerate() {
                column.push(left.value(self.left_row, index)?.clone());
            }
            for (column, value) in output.iter_mut().skip(self.left_schema.len())
                .zip(&self.right_rows[self.right_row]) {
                column.push(value.clone());
            }
            output_rows += 1;
            self.right_row += 1;
            if self.right_row == self.right_rows.len() {
                self.left_ready = false;
                self.left_row += 1;
            }
        }
        Ok(Some(Batch::from_validated_columns(
            self.output_schema.clone(), output, output_rows,
        )?))
    }
}

impl Iterator for CrossJoinStream {
    type Item = Result<Batch>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped { return None; }
        if let Some(error) = self.pending_error.take() {
            self.stopped = true;
            return Some(Err(error));
        }
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

#[cfg(test)]
mod column_major_cross_join_perf_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn zero_width_cross_product_keeps_cardinality_without_row_vectors() {
        let schema = Schema::new(vec![]).unwrap();
        let left = Batch::new_with_row_count(schema.clone(), vec![], 2).unwrap();
        let right = Batch::new_with_row_count(schema.clone(), vec![], 3).unwrap();
        let results = CrossJoinStream::new(
            Box::new([Ok(left)].into_iter()), Box::new([Ok(right)].into_iter()),
            schema.clone(), schema, 2, QueryControl::default()
        ).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(results.iter().map(Batch::num_rows).collect::<Vec<_>>(), vec![2, 2, 2]);
        assert!(results.iter().all(|batch| batch.schema().is_empty()));
    }

    #[test]
    fn wide_nullable_rows_keep_column_order_and_nulls() {
        let schema = Schema::new(vec![
            Field::new("n", DataType::Integer, true),
            Field::new("t", DataType::Text, true),
        ]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Null, Value::Text("a".repeat(256))],
            vec![Value::Integer(2), Value::Null],
        ]).unwrap();
        let right = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(3), Value::Null],
            vec![Value::Null, Value::Text("z".repeat(257))],
        ]).unwrap();
        let expected = join::cross_join(&left, &right).unwrap().rows().unwrap();
        let actual = CrossJoinStream::new(
            Box::new([Ok(left)].into_iter()), Box::new([Ok(right)].into_iter()),
            schema.clone(), schema, 3, QueryControl::default()
        ).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(actual.iter().flat_map(|batch| batch.rows().unwrap())
            .collect::<Vec<_>>(), expected);
    }
}

#[cfg(test)]
mod borrowed_cross_join_left_rows_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn reuses_wide_left_row_across_right_batches_without_changing_cartesian_result() {
        let left_schema = Schema::new(vec![
            Field::new("left_payload", DataType::Text, false),
            Field::new("left_id", DataType::Integer, false),
        ]).unwrap();
        let right_schema = Schema::new(vec![
            Field::new("right_id", DataType::Integer, false),
        ]).unwrap();
        let wide = "é🙂".repeat(2048);
        let left = Batch::from_rows(left_schema.clone(), vec![
            vec![Value::Text(wide.clone()), Value::Integer(2)],
            vec![Value::Text("other".into()), Value::Integer(3)],
        ]).unwrap();
        let right = Batch::from_rows(right_schema.clone(), vec![
            vec![Value::Integer(5)], vec![Value::Integer(8)],
            vec![Value::Integer(13)], vec![Value::Integer(21)],
        ]).unwrap();
        let expected = join::cross_join(&left, &right).unwrap().rows().unwrap();
        let stream = CrossJoinStream::new(
            Box::new([Ok(left)].into_iter()),
            Box::new([Ok(right)].into_iter()),
            left_schema, right_schema, 2, QueryControl::default(),
        ).unwrap();
        let actual = stream.collect::<Result<Vec<_>>>().unwrap().into_iter()
            .flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
}

#[cfg(test)]
mod cross_join_terminal_prefix_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn forwards_valid_product_before_later_left_failure() {
        let schema = Schema::new(vec![Field::new("k", DataType::Integer, true)]).unwrap();
        let first = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)],
        ]).unwrap();
        let right = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(10)], vec![Value::Integer(20)],
        ]).unwrap();
        let mut stream = CrossJoinStream::new(
            Box::new([Ok(first), Err(Error::Execution("late failure".into()))].into_iter()),
            Box::new([Ok(right)].into_iter()),
            schema.clone(), schema, 16, QueryControl::default(),
        ).unwrap();
        let prefix = stream.next().unwrap().unwrap();
        assert_eq!(prefix.rows().unwrap(), vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(1), Value::Integer(20)],
            vec![Value::Integer(2), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
        ]);
        assert!(matches!(stream.next(), Some(Err(Error::Execution(_)))));
        assert!(stream.next().is_none());
    }

    #[test]
    fn forwards_valid_product_before_later_schema_failure() {
        let schema = Schema::new(vec![Field::new("k", DataType::Integer, true)]).unwrap();
        let good = Batch::from_rows(schema.clone(), vec![vec![Value::Integer(1)]]).unwrap();
        let wrong = Batch::empty(Schema::new(vec![]).unwrap());
        let right = Batch::from_rows(schema.clone(), vec![vec![Value::Integer(3)]]).unwrap();
        let mut stream = CrossJoinStream::new(
            Box::new([Ok(good), Ok(wrong)].into_iter()),
            Box::new([Ok(right)].into_iter()),
            schema.clone(), schema, 8, QueryControl::default(),
        ).unwrap();
        assert_eq!(stream.next().unwrap().unwrap().num_rows(), 1);
        assert!(matches!(stream.next(), Some(Err(Error::Schema(_)))));
        assert!(stream.next().is_none());
    }
}

#[cfg(test)]
mod trusted_cross_join_output_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn streamed_batches_match_materialized_cartesian_product() {
        let left_schema = Schema::new(vec![
            Field::new("l", DataType::Text, true),
        ]).unwrap();
        let right_schema = Schema::new(vec![
            Field::new("r", DataType::Integer, true),
        ]).unwrap();
        let left = Batch::from_rows(left_schema.clone(), vec![
            vec![Value::Text("界".repeat(1024))], vec![Value::Null],
        ]).unwrap();
        let right = Batch::from_rows(right_schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Null],
        ]).unwrap();
        let expected = join::cross_join(&left, &right).unwrap();
        let parts = CrossJoinStream::new(
            Box::new([Ok(left)].into_iter()), Box::new([Ok(right)].into_iter()),
            left_schema, right_schema, 1, QueryControl::default()
        ).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(parts.iter().flat_map(|b| b.rows().unwrap())
            .collect::<Vec<_>>(), expected.rows().unwrap());
        assert!(parts.iter().all(|b| b.schema() == expected.schema()));
    }
}


#[cfg(test)]
mod cloned_stream_cross_join_budget_regressions {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn large_unused_left_string_capacity_does_not_reject_small_output() {
        let schema = Schema::new(vec![Field::new("payload", DataType::Text, false)]).unwrap();
        let mut source = String::with_capacity(MAX_OUTPUT_BATCH_BYTES + 1024);
        source.push_str("tiny");
        let left = Batch::from_rows(schema.clone(),
            vec![vec![Value::Text(source)]]).unwrap();
        let right = Batch::from_rows(schema.clone(),
            vec![vec![Value::Text("right".into())]]).unwrap();
        let mut stream = CrossJoinStream::new(
            Box::new(std::iter::once(Ok(left))),
            Box::new(std::iter::once(Ok(right))),
            schema.clone(), schema, 2, QueryControl::default(),
        ).unwrap();
        assert_eq!(stream.next().unwrap().unwrap().rows().unwrap(), vec![
            vec![Value::Text("tiny".into()), Value::Text("right".into())]
        ]);
        assert!(stream.next().is_none());
    }

    #[test]
    fn genuinely_oversized_output_is_rejected_before_cloning() {
        let schema = Schema::new(vec![Field::new("payload", DataType::Text, false)]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("x".repeat(MAX_OUTPUT_BATCH_BYTES + 1024))]
        ]).unwrap();
        let right = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("right".into())]
        ]).unwrap();
        let mut stream = CrossJoinStream::new(
            Box::new(std::iter::once(Ok(left))),
            Box::new(std::iter::once(Ok(right))),
            schema.clone(), schema, 1, QueryControl::default(),
        ).unwrap();
        assert!(stream.next().unwrap().is_err());
        assert!(stream.next().is_none());
    }
}

//! Convert fallible row streams into independently validated bounded batches.
use crate::{batch::Batch, scalar::Value, schema::Schema, Error, Result};

const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;

fn estimated_row_bytes(row: &[Value], base: usize) -> usize {
    row.iter().fold(base, |used, value| {
        used.saturating_add(match value {
            Value::Text(text) => text.capacity(),
            _ => 0,
        })
    })
}

pub struct RowBatcher<I> {
    input: I,
    schema: Schema,
    row_width: usize,
    row_limit: usize,
    pending_error: Option<Error>,
    pending_row: Option<Vec<Value>>,
    max_batch_bytes: usize,
    finished: bool,
}

impl<I> RowBatcher<I>
where I: Iterator<Item = Result<Vec<Value>>>
{
    pub fn new(input: I, schema: Schema, batch_rows: usize) -> Result<Self> {
        Self::with_byte_budget(input, schema, batch_rows, MAX_BATCH_BYTES)
    }

    fn with_byte_budget(
        input: I, schema: Schema, batch_rows: usize, max_batch_bytes: usize,
    ) -> Result<Self> {
        let minimum = schema.len().checked_mul(std::mem::size_of::<Value>())
            .ok_or_else(|| Error::Execution("row batch width overflow".into()))?;
        if batch_rows == 0 || max_batch_bytes == 0 || minimum > max_batch_bytes {
            return Err(Error::Execution("invalid row batch size or byte budget".into()));
        }
        let row_width = minimum.max(1);
        let row_limit = batch_rows.min(65_536)
            .min((max_batch_bytes / row_width).max(1));
        Ok(Self {
            input, schema, row_width, row_limit, pending_error: None,
            pending_row: None, max_batch_bytes, finished: false,
        })
    }
}

impl<I> Iterator for RowBatcher<I>
where I: Iterator<Item = Result<Vec<Value>>>
{
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished { return None; }
        if let Some(error) = self.pending_error.take() {
            self.finished = true;
            return Some(Err(error));
        }

        let row_width = self.row_width;
        let limit = self.row_limit;
        let mut retained_bytes = 0usize;
        // Exact or bounded iterators often have far fewer rows remaining than
        // the requested batch size. Avoid reserving tens of thousands of slots
        // for a tiny final batch.
        let capacity = self.input.size_hint().1
            .map_or(limit.min(256), |upper| upper.min(limit));
        let mut columns = (0..self.schema.len())
            .map(|_| Vec::with_capacity(capacity)).collect::<Vec<Vec<Value>>>();
        let mut row_count = 0usize;
        while row_count < limit {
            let next = self.pending_row.take().map(Ok).or_else(|| self.input.next());
            match next {
                Some(Ok(row)) => {
                    if row.len() != self.schema.len() {
                        let error = Error::Schema("row width does not match schema".into());
                        if row_count == 0 {
                            self.finished = true;
                            return Some(Err(error));
                        }
                        self.pending_error = Some(error);
                        break;
                    }
                    // Validate each incoming row before adding it to the current
                    // batch. A late invalid cell must not discard earlier good rows
                    // when Batch::new_with_row_count validates the final columns.
                    if let Some(field) = self.schema.fields().iter().zip(&row)
                        .find_map(|(field, value)| {
                            (!field.data_type.accepts(value) ||
                                (!field.nullable && value.is_null())).then_some(field)
                        })
                    {
                        let error = Error::Type(format!("invalid value in column {}", field.name));
                        if row_count == 0 {
                            self.finished = true;
                            return Some(Err(error));
                        }
                        self.pending_error = Some(error);
                        break;
                    }
                    let row_bytes = estimated_row_bytes(&row, row_width);
                    if row_bytes > self.max_batch_bytes {
                        let error = Error::Execution("row exceeds native batch byte budget".into());
                        if row_count == 0 {
                            self.finished = true;
                            return Some(Err(error));
                        }
                        self.pending_error = Some(error);
                        break;
                    }
                    if row_count > 0 &&
                        retained_bytes.saturating_add(row_bytes) > self.max_batch_bytes {
                        self.pending_row = Some(row);
                        break;
                    }
                    retained_bytes += row_bytes;
                    for (column, value) in columns.iter_mut().zip(row) {
                        column.push(value);
                    }
                    row_count += 1;
                },
                Some(Err(error)) => {
                    if row_count == 0 {
                        self.finished = true;
                        return Some(Err(error));
                    }
                    self.pending_error = Some(error);
                    break;
                }
                None => {
                    self.finished = true;
                    break;
                }
            }
        }
        if row_count == 0 { return None; }
        // Every incoming cell was checked against this schema before admission.
        let batch = Batch::from_validated_columns(self.schema.clone(), columns, row_count);
        if batch.is_err() { self.finished = true; }
        Some(batch)
    }
}

pub struct BatchChunks<'a>(crate::batch_chunks::BatchChunks<'a>);

impl<'a> BatchChunks<'a> {
    pub fn new(input: &'a Batch, batch_rows: usize) -> Result<Self> {
        if batch_rows == 0 {
            return Err(Error::Execution("batch size must be positive".into()));
        }
        Ok(Self(crate::batch_chunks::chunks(input, batch_rows)?))
    }
}

impl Iterator for BatchChunks<'_> {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.0.nth(n)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }

    fn count(self) -> usize {
        self.0.count()
    }

    fn last(self) -> Option<Self::Item> {
        self.0.last()
    }
}

impl ExactSizeIterator for BatchChunks<'_> {}
impl std::iter::FusedIterator for BatchChunks<'_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, Field};

    fn schema() -> Schema {
        Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap()
    }

    #[test]
    fn row_batched_input_never_exceeds_target_size() {
        let rows = (0..1005).map(|n| Ok(vec![Value::Integer(n)]));
        let batches = RowBatcher::new(rows, schema(), 128).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches.iter().map(Batch::num_rows).sum::<usize>(), 1005);
        assert!(batches.iter().all(|b| b.num_rows() <= 128));
        assert_eq!(batches.last().unwrap().row(108).unwrap(), vec![Value::Integer(1004)]);
    }

    #[test]
    fn buffered_rows_are_not_lost_before_errors() {
        let input = vec![Ok(vec![Value::Integer(7)]), Err(Error::Parse("bad row".into()))];
        let mut batches = RowBatcher::new(input.into_iter(), schema(), 4).unwrap();
        assert_eq!(batches.next().unwrap().unwrap().num_rows(), 1);
        assert!(batches.next().unwrap().is_err());
        assert!(batches.next().is_none());
    }
}

#[cfg(test)]
mod batcher_malformed_width_tests {
    use super::*;

    #[test]
    fn returns_valid_prefix_then_terminal_width_error() {
        let schema = Schema::new(vec![
            crate::schema::Field::new("n", crate::schema::DataType::Integer, false)
        ]).unwrap();
        let input = vec![
            Ok(vec![Value::Integer(5)]),
            Ok(vec![]),
            Ok(vec![Value::Integer(10)]),
        ];
        let mut batches = RowBatcher::new(input.into_iter(), schema, 8).unwrap();
        assert_eq!(batches.next().unwrap().unwrap().rows().unwrap(),
            vec![vec![Value::Integer(5)]]);
        assert!(batches.next().unwrap().is_err());
        assert!(batches.next().is_none());
    }
}

#[cfg(test)]
mod consolidated_bounded_row_batch_tests {
    use super::*;
    use crate::schema::{DataType, Field};
    #[test]
    fn huge_requested_batch_stays_bounded() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let rows = (0..65_537).map(|n| Ok(vec![Value::Integer(n)]));
        let batches = RowBatcher::new(rows, schema, usize::MAX)
            .unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches.iter().map(Batch::num_rows).collect::<Vec<_>>(), vec![65_536, 1]);
    }
}

#[cfg(test)]
mod scale_contiguous_chunk_perf_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn slice_iterator_preserves_nullable_data_and_zero_width_counts() {
        let source = Batch::from_rows(Schema::new(vec![Field::new("x", DataType::Text, true)]).unwrap(),
            vec![vec![Value::Null], vec![Value::Text("one".into())], vec![Value::Null]]).unwrap();
        let parts = BatchChunks::new(&source, 2).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(parts.iter().map(Batch::num_rows).collect::<Vec<_>>(), [2, 1]);
        assert_eq!(parts.iter().flat_map(|part| part.rows().unwrap()).collect::<Vec<_>>(), source.rows().unwrap());

        let zero_width = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 5).unwrap();
        let parts = BatchChunks::new(&zero_width, 2).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(parts.iter().map(Batch::num_rows).collect::<Vec<_>>(), [2, 2, 1]);
    }
}

#[cfg(test)]
mod bounded_remaining_rows_capacity_tests {
    use super::*;
    #[test]
    fn short_iterators_are_collected_with_oversized_batch_targets() {
        let schema = Schema::new(vec![crate::schema::Field::new(
            "n", crate::schema::DataType::Integer, false)]).unwrap();
        let input = (0..3).map(|n| Ok(vec![Value::Integer(n)]));
        let batches = RowBatcher::new(input, schema, 65_536).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].rows().unwrap(), vec![
            vec![Value::Integer(0)], vec![Value::Integer(1)], vec![Value::Integer(2)]
        ]);
    }
}

#[cfg(test)]
mod native_columnar_row_batched_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn columnar_batched_stream_keeps_nullable_data_and_zero_width_rows() {
        let schema = Schema::new(vec![
            Field::new("n", DataType::Integer, false),
            Field::new("s", DataType::Text, true),
        ]).unwrap();
        let rows = vec![
            Ok(vec![Value::Integer(1), Value::Null]),
            Ok(vec![Value::Integer(2), Value::Text("界".into())]),
            Ok(vec![Value::Integer(3), Value::Null]),
        ];
        let chunks = RowBatcher::new(rows.into_iter(), schema.clone(), 2).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(chunks.iter().map(Batch::num_rows).collect::<Vec<_>>(), [2, 1]);
        assert_eq!(chunks[0].column(1).unwrap(), [Value::Null, Value::Text("界".into())]);
        let zero = Schema::new(vec![]).unwrap();
        let zero_chunks = RowBatcher::new(vec![Ok(vec![]); 3].into_iter(), zero, 2).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(zero_chunks.iter().map(Batch::num_rows).collect::<Vec<_>>(), [2, 1]);
        assert!(zero_chunks[0].rows().unwrap().iter().all(Vec::is_empty));
        assert_eq!(chunks[0].schema(), &schema);
    }
}

#[cfg(test)]
mod row_batch_byte_admission_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    fn text_schema() -> Schema {
        Schema::new(vec![Field::new("payload", DataType::Text, false)]).unwrap()
    }

    #[test]
    fn large_strings_split_batches_without_dropping_rows() {
        let rows = (0..4).map(|i| Ok(vec![Value::Text(format!(
            "{i}-{}", "x".repeat(60)
        ))]));
        let batches = RowBatcher::with_byte_budget(rows, text_schema(), 100, 100)
            .unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches.iter().map(Batch::num_rows).collect::<Vec<_>>(),
            vec![1, 1, 1, 1]);
        let collected = batches.iter().flat_map(|batch| batch.rows().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(collected.len(), 4);
        for (i, row) in collected.iter().enumerate() {
            assert!(matches!(&row[0], Value::Text(text) if text.starts_with(&format!("{i}-"))));
        }
    }

    #[test]
    fn oversized_row_reports_error_after_valid_prefix() {
        let rows = vec![
            Ok(vec![Value::Text("ok".into())]),
            Ok(vec![Value::Text("x".repeat(120))]),
            Ok(vec![Value::Text("unreachable".into())]),
        ];
        let mut batches = RowBatcher::with_byte_budget(
            rows.into_iter(), text_schema(), 100, 100
        ).unwrap();
        assert_eq!(batches.next().unwrap().unwrap().num_rows(), 1);
        assert!(batches.next().unwrap().is_err());
        assert!(batches.next().is_none());
    }
}

#[cfg(test)]
mod unknown_length_batch_stream_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn unknown_length_iterator_keeps_row_order() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let mut n = 0;
        let rows = std::iter::from_fn(move || {
            if n == 300 { return None; }
            let value = n;
            n += 1;
            Some(Ok(vec![Value::Integer(value)]))
        });
        let output = RowBatcher::new(rows, schema, 4096).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(output.iter().map(Batch::num_rows).sum::<usize>(), 300);
        assert_eq!(output[0].value(299, 0).unwrap(), &Value::Integer(299));
    }
}

#[cfg(test)]
mod late_row_type_error_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    use std::{cell::Cell, rc::Rc};

    #[test]
    fn malformed_type_reports_valid_prefix_before_terminal_error() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("payload", DataType::Text, true),
        ]).unwrap();
        let rows = vec![
            Ok(vec![Value::Integer(1), Value::Text("first".into())]),
            Ok(vec![Value::Integer(2), Value::Null]),
            Ok(vec![Value::Integer(3), Value::Integer(99)]),
            Ok(vec![Value::Integer(4), Value::Text("unreachable".into())]),
        ];
        let polls = Rc::new(Cell::new(0usize));
        let counter = Rc::clone(&polls);
        let input = rows.into_iter().inspect(move |_| counter.set(counter.get() + 1));
        let mut batches = RowBatcher::new(input, schema, 8).unwrap();
        assert_eq!(batches.next().unwrap().unwrap().rows().unwrap(), vec![
            vec![Value::Integer(1), Value::Text("first".into())],
            vec![Value::Integer(2), Value::Null],
        ]);
        assert!(batches.next().unwrap().unwrap_err().to_string().contains("payload"));
        assert!(batches.next().is_none());
        assert_eq!(polls.get(), 3);
    }

    #[test]
    fn required_null_and_first_row_errors_do_not_drop_valid_prefixes() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let input = vec![Ok(vec![Value::Integer(9)]), Ok(vec![Value::Null])];
        let mut batches = RowBatcher::new(input.into_iter(), schema.clone(), 16).unwrap();
        assert_eq!(batches.next().unwrap().unwrap().row(0).unwrap(),
            vec![Value::Integer(9)]);
        assert!(batches.next().unwrap().is_err());
        assert!(batches.next().is_none());

        let mut first = RowBatcher::new(
            [Ok(vec![Value::Text("wrong".into())])].into_iter(), schema, 16,
        ).unwrap();
        assert!(first.next().unwrap().is_err());
        assert!(first.next().is_none());
    }
}

#[cfg(test)]
mod trusted_stream_finalize_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn retains_nullable_wide_text_and_reports_late_invalid_cells() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("payload", DataType::Text, true),
        ]).unwrap();
        let text = "🙂界".repeat(2048);
        let rows = vec![
            Ok(vec![Value::Integer(1), Value::Text(text.clone())]),
            Ok(vec![Value::Integer(2), Value::Null]),
            Ok(vec![Value::Integer(3), Value::Integer(9)]),
        ];
        let mut stream = RowBatcher::new(rows.into_iter(), schema, 16).unwrap();
        let batch = stream.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.value(0, 1).unwrap(), &Value::Text(text));
        assert_eq!(batch.value(1, 1).unwrap(), &Value::Null);
        assert!(stream.next().unwrap().is_err());
        assert!(stream.next().is_none());
    }
}

#[cfg(test)]
mod precomputed_row_batch_limits_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn repeated_batches_keep_exact_row_and_byte_boundaries() {
        let schema = Schema::new(vec![Field::new("v", DataType::Integer, false)]).unwrap();
        let width = std::mem::size_of::<Value>();
        let batches = RowBatcher::with_byte_budget(
            (0..10).map(|i| Ok(vec![Value::Integer(i)])),
            schema.clone(), 100, 3 * width,
        ).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches.iter().map(Batch::num_rows).collect::<Vec<_>>(), [3, 3, 3, 1]);
        assert_eq!(batches.last().unwrap().row(0).unwrap(), vec![Value::Integer(9)]);

        let zero_width = Schema::new(vec![]).unwrap();
        let parts = RowBatcher::with_byte_budget(
            (0..3).map(|_| Ok(vec![])), zero_width, 10, 1,
        ).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(parts.iter().map(Batch::num_rows).collect::<Vec<_>>(), [1, 1, 1]);
        assert!(RowBatcher::with_byte_budget(
            std::iter::empty(), schema, 10, width - 1,
        ).is_err());
    }
}

#[cfg(test)]
mod retained_capacity_admission_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn large_retained_text_capacity_fails_after_valid_rows() {
        let schema = Schema::new(vec![Field::new("text", DataType::Text, false)]).unwrap();
        let mut oversized = String::with_capacity(4096);
        oversized.push('x');
        let rows = vec![
            Ok(vec![Value::Text("ok".into())]),
            Ok(vec![Value::Text(oversized)]),
            Ok(vec![Value::Text("unreachable".into())]),
        ];
        let mut batches = RowBatcher::with_byte_budget(rows.into_iter(), schema, 8, 128).unwrap();
        assert_eq!(batches.next().unwrap().unwrap().rows().unwrap(),
            vec![vec![Value::Text("ok".into())]]);
        assert!(batches.next().unwrap().is_err());
        assert!(batches.next().is_none());
    }

    #[test]
    fn capacity_equal_to_budget_is_accepted() {
        let schema = Schema::new(vec![Field::new("text", DataType::Text, false)]).unwrap();
        let mut text = String::with_capacity(64);
        text.push('x');
        let minimum = std::mem::size_of::<Value>();
        let capacity = text.capacity();
        let batches = RowBatcher::with_byte_budget(
            [Ok(vec![Value::Text(text)])].into_iter(), schema, 4,
            minimum + capacity,
        ).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }
}

#[cfg(test)]
mod scale_chunk_iterator_adaptor_regressions {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn wrapper_preserves_exact_size_and_skip_behavior() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let input = Batch::from_rows(schema, (0..23)
            .map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let mut chunks = BatchChunks::new(&input, 4).unwrap();
        assert_eq!(chunks.len(), 6);
        assert_eq!(chunks.size_hint(), (6, Some(6)));
        assert_eq!(chunks.nth(2).unwrap().unwrap().row(0).unwrap(), vec![Value::Integer(8)]);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks.last().unwrap().unwrap().row(0).unwrap(), vec![Value::Integer(20)]);
        assert_eq!(BatchChunks::new(&input, 4).unwrap().count(), 6);
        let mut exhausted = BatchChunks::new(&input, 4).unwrap();
        assert!(exhausted.nth(usize::MAX).is_none());
        assert!(exhausted.next().is_none());
    }
}

#[cfg(test)]
mod row_batch_retained_capacity_regressions {
    use super::*;
    use crate::schema::{DataType, Field};

    fn text_schema() -> Schema {
        Schema::new(vec![Field::new("payload", DataType::Text, false)]).unwrap()
    }

    fn sparse_text(capacity: usize) -> Vec<Value> {
        let mut text = String::with_capacity(capacity);
        text.push('x');
        vec![Value::Text(text)]
    }

    #[test]
    fn splits_sparse_strings_by_retained_allocation_not_string_length() {
        let input = (0..3).map(|_| Ok(sparse_text(60)));
        let batches = RowBatcher::with_byte_budget(input, text_schema(), 10, 100)
            .unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches.iter().map(Batch::num_rows).collect::<Vec<_>>(), [1, 1, 1]);
        assert!(batches.iter().all(|batch| batch.value(0, 0).unwrap()
            == &Value::Text("x".into())));
    }

    #[test]
    fn oversized_retained_allocation_reports_error_after_valid_prefix() {
        let rows = vec![Ok(sparse_text(1)), Ok(sparse_text(200))];
        let mut stream = RowBatcher::with_byte_budget(rows.into_iter(), text_schema(), 10, 100)
            .unwrap();
        assert_eq!(stream.next().unwrap().unwrap().num_rows(), 1);
        assert!(stream.next().unwrap().unwrap_err().to_string().contains("byte budget"));
        assert!(stream.next().is_none());
    }
}

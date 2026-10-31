//! Bounded scan cursor over a native table without materializing full results.
use crate::{
    batch::Batch, expr::Expr, scalar::Value, schema::Schema,
    Error, Result,
};

pub struct ScanCursor<'a> {
    source: &'a Batch,
    predicate: Option<Expr>,
    columns: Vec<usize>,
    output_schema: Schema,
    identity_projection: bool,
    target_rows: usize,
    next_row: usize,
    pending_error: Option<Error>,
    failed: bool,
}

pub fn scan<'a>(
    source: &'a Batch,
    predicate: Option<Expr>,
    projected_columns: &[&str],
    target_rows: usize,
) -> Result<ScanCursor<'a>> {
    if target_rows == 0 {
        return Err(Error::Execution("scan batch size must be positive".into()));
    }
    if let Some(predicate) = &predicate {
        let kind = predicate.output_type(source.schema())?;
        if kind != crate::schema::DataType::Boolean &&
            !matches!(predicate, Expr::Literal(Value::Null)) {
            return Err(Error::Type("scan predicate must be boolean".into()));
        }
    }
    // A constant true predicate cannot reject rows. Treat it as unfiltered
    // so identity scans can take the contiguous batch slicing path.
    let predicate = match predicate {
        Some(Expr::Literal(Value::Boolean(true))) => None,
        other => other,
    };
    let columns = if projected_columns.is_empty() {
        (0..source.schema().len()).collect::<Vec<_>>()
    } else {
        projected_columns.iter().map(|name| source.schema().column(name))
            .collect::<Result<Vec<_>>>()?
    };
    let output_schema = Schema::new(columns.iter()
        .map(|&index| Ok(source.schema().field(index)?.clone()))
        .collect::<Result<Vec<_>>>()?)?;
    let identity_projection = columns.iter().copied().eq(0..source.schema().len());
    // FALSE and UNKNOWN cannot pass WHERE, regardless of source cardinality.
    let next_row = if matches!(&predicate,
        Some(Expr::Literal(Value::Boolean(false) | Value::Null)))
    {
        source.num_rows()
    } else {
        0
    };
    Ok(ScanCursor {
        source, predicate, columns, output_schema, identity_projection,
        target_rows, next_row, pending_error: None, failed: false,
    })
}

impl Iterator for ScanCursor<'_> {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed { return None; }
        if let Some(error) = self.pending_error.take() {
            self.failed = true;
            return Some(Err(error));
        }
        if self.predicate.is_none() {
            if self.next_row == self.source.num_rows() { return None; }
            let start = self.next_row;
            let end = start.saturating_add(self.target_rows).min(self.source.num_rows());
            self.next_row = end;
            // Unfiltered projections always select a contiguous window.
            // Slice the requested columns directly instead of allocating
            // row coordinates and gathering each value one by one.
            let result = if self.identity_projection {
                self.source.slice_rows(start, end)
            } else {
                self.columns.iter().map(|&column| {
                    Ok(self.source.column(column)?[start..end].to_vec())
                }).collect::<Result<Vec<_>>>()
                    .and_then(|columns| Batch::new_with_row_count(
                        self.output_schema.clone(), columns, end - start,
                    ))
            };
            if result.is_err() { self.failed = true; }
            return Some(result);
        }
        let mut selected = Vec::with_capacity(self.target_rows.min(self.source.num_rows()));
        while self.next_row < self.source.num_rows() && selected.len() < self.target_rows {
            let row = self.next_row;
            self.next_row += 1;
            let include = match &self.predicate {
                Some(predicate) => match predicate.evaluate(self.source, row)
                    .and_then(|value| value.as_boolean()) {
                    Ok(result) => result == Some(true),
                    Err(error) => {
                        if selected.is_empty() {
                            self.failed = true;
                            return Some(Err(error));
                        }
                        // Deliver matches already evaluated successfully.
                        // Do not evaluate rows after the terminal error.
                        self.pending_error = Some(error);
                        break;
                    },
                },
                None => true,
            };
            if include { selected.push(row); }
        }
        if selected.is_empty() {
            return None;
        }
        let output = self.columns.iter().map(|&column| {
            let values = self.source.column(column)?;
            Ok(selected.iter().map(|&row| values[row].clone()).collect::<Vec<Value>>())
        }).collect::<Result<Vec<_>>>()
            .and_then(|columns| Batch::new_with_row_count(
                self.output_schema.clone(), columns, selected.len(),
            ));
        if output.is_err() { self.failed = true; }
        Some(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{expr::BinaryOp, schema::{DataType, Field}};

    #[test]
    fn rejects_bad_predicates_on_empty_sources() {
        let source = Batch::empty(Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap());
        assert!(scan(&source, Some(Expr::Column("missing".into())), &[], 10).is_err());
        assert!(scan(&source, Some(Expr::Column("id".into())), &[], 10).is_err());
        assert!(scan(&source, Some(Expr::Literal(Value::Null)), &[], 10).is_ok());
    }

    #[test]
    fn scan_filters_before_materializing_projected_columns() {
        let source = Batch::from_rows(
            Schema::new(vec![
                Field::new("id", DataType::Integer, false),
                Field::new("flag", DataType::Boolean, true),
            ]).unwrap(),
            vec![
                vec![Value::Integer(1), Value::Boolean(true)],
                vec![Value::Integer(2), Value::Null],
                vec![Value::Integer(3), Value::Boolean(false)],
                vec![Value::Integer(4), Value::Boolean(true)],
            ],
        ).unwrap();
        let predicate = Expr::binary(
            Expr::Column("flag".into()), BinaryOp::Equal,
            Expr::Literal(Value::Boolean(true)),
        );
        let chunks = scan(&source, Some(predicate), &["id"], 1).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].rows().unwrap(), vec![vec![Value::Integer(1)]]);
        assert_eq!(chunks[1].rows().unwrap(), vec![vec![Value::Integer(4)]]);
        assert!(scan(&source, None, &[], 0).is_err());
    }
}


/// A globally paginated native scan, maintaining offset across batch boundaries.
pub struct WindowedScan<'a> {
    inner: ScanCursor<'a>,
    skip: usize,
    remaining: usize,
    stopped: bool,
}

pub fn scan_window<'a>(
    source: &'a Batch,
    predicate: Option<Expr>,
    projected_columns: &[&str],
    target_rows: usize,
    offset: usize,
    count: usize,
) -> Result<WindowedScan<'a>> {
    offset.checked_add(count)
        .ok_or_else(|| Error::Execution("scan window OFFSET + LIMIT overflow".into()))?;
    Ok(WindowedScan {
        inner: scan(source, predicate, projected_columns, target_rows)?,
        skip: offset,
        remaining: count,
        stopped: false,
    })
}

impl Iterator for WindowedScan<'_> {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped || self.remaining == 0 { return None; }
        loop {
            let batch = match self.inner.next()? {
                Ok(batch) => batch,
                Err(error) => {
                    self.stopped = true;
                    return Some(Err(error));
                }
            };
            if self.skip >= batch.num_rows() {
                self.skip -= batch.num_rows();
                continue;
            }
            let available = batch.num_rows() - self.skip;
            let count = self.remaining.min(available);
            let offset = self.skip;
            self.skip = 0;
            self.remaining -= count;
            return Some(crate::limit::limit(&batch, offset, Some(count)));
        }
    }
}

#[cfg(test)]
mod scan_window_feature_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn applies_global_offset_after_filtering_and_projects_rows() {
        let source = Batch::from_rows(Schema::new(vec![
            Field::new("n", DataType::Integer, false),
        ]).unwrap(), (0..12).map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let filter = Expr::binary(Expr::Column("n".into()), crate::expr::BinaryOp::GreaterEqual,
            Expr::Literal(Value::Integer(3)));
        let result = scan_window(&source, Some(filter), &["n"], 2, 3, 4).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        let rows = result.iter().flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(rows, vec![
            vec![Value::Integer(6)], vec![Value::Integer(7)],
            vec![Value::Integer(8)], vec![Value::Integer(9)]
        ]);
        assert_eq!(scan_window(&source, None, &[], 3, 0, 0).unwrap().count(), 0);
        assert!(scan_window(&source, None, &[], 0, 0, 1).is_err());
    }
}


#[cfg(test)]
mod scan_failure_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn predicate_errors_terminate_the_scan() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(0)], vec![Value::Integer(2)]],
        ).unwrap();
        let division = Expr::binary(Expr::Literal(Value::Integer(1)),
            crate::expr::BinaryOp::Divide, Expr::Column("n".into()));
        let predicate = Expr::binary(division, crate::expr::BinaryOp::Greater,
            Expr::Literal(Value::Float(0.0)));
        let mut cursor = scan(&input, Some(predicate), &[], 1).unwrap();
        assert!(cursor.next().unwrap().is_err());
        assert!(cursor.next().is_none());
    }
}

#[cfg(test)]
mod identity_scan_fast_path_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn identity_scan_preserves_chunking_and_rows() {
        let input = Batch::from_rows(Schema::new(vec![
            Field::new("key", DataType::Text, true),
        ]).unwrap(), vec![
            vec![Value::Null], vec![Value::Text("a".into())],
            vec![Value::Text("b".into())], vec![Value::Null],
        ]).unwrap();
        let parts = scan(&input, None, &[], 3).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(parts.iter().map(Batch::num_rows).collect::<Vec<_>>(), [3, 1]);
        assert_eq!(parts.iter().flat_map(|p| p.rows().unwrap()).collect::<Vec<_>>(), input.rows().unwrap());
    }
}

#[cfg(test)]
mod constant_true_scan_perf_tests {
    use super::*;
    #[test]
    fn true_predicate_uses_the_same_chunks_as_unfiltered_scan() {
        let schema = Schema::new(vec![crate::schema::Field::new(
            "id", crate::schema::DataType::Integer, false)]).unwrap();
        let input = Batch::from_rows(schema, (0..7)
            .map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let unfiltered = scan(&input, None, &[], 3).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        let constant = scan(&input, Some(Expr::Literal(Value::Boolean(true))), &[], 3).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(constant, unfiltered);
        assert_eq!(constant.iter().map(Batch::num_rows).collect::<Vec<_>>(), [3, 3, 1]);
    }
}

#[cfg(test)]
mod native_window_overflow_tests {
    use super::*;
    use crate::schema::{Field, DataType, Schema};

    #[test]
    fn invalid_offsets_are_rejected_without_scan_execution() {
        let source = Batch::empty(Schema::new(vec![
            Field::new("n", DataType::Integer, false),
        ]).unwrap());
        assert!(scan_window(&source, None, &[], 10, usize::MAX, 1).is_err());
        assert!(scan_window(&source, None, &[], 10, usize::MAX - 1, 2).is_err());
        assert!(scan_window(&source, None, &[], 10, usize::MAX, 0).is_ok());
    }
}

#[cfg(test)]
mod constant_false_scan_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn rejects_all_rows_without_losing_validation() {
        let source = Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap(),
            (0..100).map(|n| vec![Value::Integer(n)]).collect(),
        ).unwrap();
        for predicate in [Value::Boolean(false), Value::Null] {
            let mut cursor = scan(&source, Some(Expr::Literal(predicate.clone())), &["n"], 1).unwrap();
            assert!(cursor.next().is_none());
            assert!(cursor.next().is_none());
            assert!(scan(&source, Some(Expr::Literal(predicate)), &["missing"], 1).is_err());
        }
    }
}

#[cfg(test)]
mod contiguous_projected_scan_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn unfiltered_projections_match_reference_for_all_chunk_sizes() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("payload", DataType::Text, true),
            Field::new("flag", DataType::Boolean, true),
        ]).unwrap();
        let source = Batch::from_rows(schema, (0..163).map(|index| vec![
            Value::Integer(index),
            if index % 13 == 0 { Value::Null }
            else { Value::Text(format!("row-{index}-{}", "🙂".repeat(20))) },
            if index % 5 == 0 { Value::Null }
            else { Value::Boolean(index % 2 == 0) },
        ]).collect()).unwrap();

        for projection in [
            vec!["payload"], vec!["flag", "id"], vec!["id", "payload", "flag"],
        ] {
            for chunk_size in [1, 7, 32, 163, 500] {
                let result = scan(&source, None, &projection, chunk_size).unwrap()
                    .collect::<Result<Vec<_>>>().unwrap();
                let flattened = result.iter().flat_map(|batch| batch.rows().unwrap())
                    .collect::<Vec<_>>();
                let indices = projection.iter().map(|name| source.schema().column(name).unwrap())
                    .collect::<Vec<_>>();
                let expected = (0..source.num_rows()).map(|row| {
                    indices.iter().map(|&column| source.value(row, column).unwrap().clone())
                        .collect::<Vec<_>>()
                }).collect::<Vec<_>>();
                assert_eq!(flattened, expected);
                assert!(result.iter().all(|part| part.num_rows() <= chunk_size));
                assert!(result.iter().all(|part| part.schema().len() == indices.len()));
            }
        }
    }

    #[test]
    fn constant_true_uses_same_projected_path_and_empty_scan_fuses() {
        let source = Batch::from_rows(Schema::new(vec![
            Field::new("first", DataType::Integer, false),
            Field::new("second", DataType::Text, true),
        ]).unwrap(), vec![
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Text("é".repeat(1000))],
        ]).unwrap();
        let projected = scan(&source, Some(Expr::Literal(Value::Boolean(true))),
            &["second"], 1).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(projected.len(), 2);
        assert_eq!(projected[0].row(0).unwrap(), vec![Value::Null]);
        assert_eq!(projected[1].row(0).unwrap(), vec![Value::Text("é".repeat(1000))]);
        let empty = Batch::empty(source.schema().clone());
        let mut cursor = scan(&empty, None, &["second"], 1).unwrap();
        assert!(cursor.next().is_none());
        assert!(cursor.next().is_none());
    }
}

#[cfg(test)]
mod predicate_error_deferred_prefix_tests {
    use super::*;
    use crate::{expr::BinaryOp, schema::{DataType, Field}};

    fn input() -> Batch {
        let schema = Schema::new(vec![
            Field::new("num", DataType::Integer, false),
            Field::new("den", DataType::Integer, false),
        ]).unwrap();
        Batch::from_rows(schema, vec![
            vec![Value::Integer(5), Value::Integer(1)],
            vec![Value::Integer(8), Value::Integer(2)],
            vec![Value::Integer(7), Value::Integer(0)],
            vec![Value::Integer(9), Value::Integer(3)],
        ]).unwrap()
    }

    fn predicate() -> Expr {
        Expr::binary(Expr::binary(Expr::Column("num".into()),
            BinaryOp::Divide, Expr::Column("den".into())),
            BinaryOp::Greater, Expr::Literal(Value::Float(0.0)))
    }

    #[test]
    fn returns_valid_selected_prefix_then_exact_terminal_error() {
        let batch = input();
        let mut scan = scan(&batch, Some(predicate()), &["num"], 10).unwrap();
        assert_eq!(scan.next().unwrap().unwrap().rows().unwrap(), vec![
            vec![Value::Integer(5)], vec![Value::Integer(8)]
        ]);
        assert!(scan.next().unwrap().unwrap_err().to_string()
            .contains("division by zero"));
        assert!(scan.next().is_none());
    }

    #[test]
    fn pagination_does_not_evaluate_rows_beyond_fulfilled_limit() {
        let batch = input();
        let result = scan_window(&batch, Some(predicate()), &["num"], 10, 0, 1)
            .unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(result[0].rows().unwrap(), vec![vec![Value::Integer(5)]]);
    }
}

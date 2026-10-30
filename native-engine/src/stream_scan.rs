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
    Ok(ScanCursor {
        source, predicate, columns, output_schema, identity_projection,
        target_rows, next_row: 0, failed: false,
    })
}

impl Iterator for ScanCursor<'_> {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed { return None; }
        // WHERE FALSE and WHERE NULL cannot select a row, regardless of input size.
        if matches!(&self.predicate,
            Some(Expr::Literal(Value::Boolean(false) | Value::Null))) {
            return None;
        }
        if self.predicate.is_none() && self.identity_projection {
            if self.next_row == self.source.num_rows() { return None; }
            let end = self.next_row.saturating_add(self.target_rows).min(self.source.num_rows());
            let result = self.source.slice_rows(self.next_row, end);
            self.next_row = end;
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
                        self.failed = true;
                        return Some(Err(error));
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
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]],
        ).unwrap();
        let mut cursor = scan(&input, Some(Expr::Column("n".into())), &[], 1).unwrap();
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
mod constant_rejected_scan_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn false_and_null_filters_never_emit_rows_even_for_large_sources() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap(),
            (0..1000).map(|n| vec![Value::Integer(n)]).collect(),
        ).unwrap();
        for value in [Value::Boolean(false), Value::Null] {
            let mut cursor = scan(&input, Some(Expr::Literal(value)), &[], 16).unwrap();
            assert!(cursor.next().is_none());
            assert!(cursor.next().is_none());
        }
        assert_eq!(scan(&input, None, &[], 16).unwrap().count(), 63);
    }
}

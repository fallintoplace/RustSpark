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
    target_rows: usize,
    next_row: usize,
    stopped: bool,
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
    let columns = if projected_columns.is_empty() {
        (0..source.schema().len()).collect::<Vec<_>>()
    } else {
        projected_columns.iter().map(|name| source.schema().column(name))
            .collect::<Result<Vec<_>>>()?
    };
    let output_schema = Schema::new(columns.iter()
        .map(|&index| Ok(source.schema().field(index)?.clone()))
        .collect::<Result<Vec<_>>>()?)?;
    Ok(ScanCursor {
        source, predicate, columns, output_schema, target_rows, next_row: 0, stopped: false,
    })
}

impl Iterator for ScanCursor<'_> {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped { return None; }
        let mut selected = Vec::with_capacity(self.target_rows.min(self.source.num_rows()));
        while self.next_row < self.source.num_rows() && selected.len() < self.target_rows {
            let row = self.next_row;
            self.next_row += 1;
            let include = match &self.predicate {
                Some(predicate) => match predicate.evaluate(self.source, row)
                    .and_then(|value| value.as_boolean()) {
                    Ok(result) => result == Some(true),
                    Err(error) => {
                        self.stopped = true;
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
            selected.iter()
                .map(|&row| self.source.value(row, column).cloned())
                .collect::<Result<Vec<Value>>>()
        }).collect::<Result<Vec<_>>>()
            .and_then(|columns| Batch::new(self.output_schema.clone(), columns));
        Some(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{expr::BinaryOp, schema::{DataType, Field}};

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
    #[test]
    fn failed_scan_predicates_stop_the_cursor() {
        let source = Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]],
        ).unwrap();
        let mut cursor = scan(&source, Some(Expr::Column("n".into())), &[], 1).unwrap();
        assert!(cursor.next().unwrap().is_err());
        assert!(cursor.next().is_none());
    }
}

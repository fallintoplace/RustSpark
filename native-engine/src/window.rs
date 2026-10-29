use std::{cmp::Ordering, collections::HashMap};

use crate::{
    batch::Batch, expr::Expr, scalar::Value, schema::{DataType, Field, Schema},
    sort::{compare_values, SortKey}, Error, Result,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowKind { RowNumber, Rank, DenseRank }

#[derive(Clone, Debug, PartialEq)]
pub struct WindowSpec {
    pub kind: WindowKind,
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<SortKey>,
    pub alias: String,
}

fn compare_keys(left: &[Value], right: &[Value], keys: &[SortKey]) -> Ordering {
    for ((left, right), key) in left.iter().zip(right).zip(keys) {
        let comparison = compare_values(left, right, key.descending, key.nulls_first);
        if comparison != Ordering::Equal { return comparison; }
    }
    Ordering::Equal
}

/// Evaluate Spark ranking windows while preserving input row order.
/// Each partition is sorted independently. Rank counts tied positions,
/// whereas dense_rank counts distinct sort keys.
pub fn evaluate(input: &Batch, specs: &[WindowSpec]) -> Result<Batch> {
    if specs.len() > 64 { return Err(Error::Execution("too many window expressions".into())); }
    let mut output = input.clone();
    for spec in specs {
        if spec.order_by.is_empty() {
            return Err(Error::Parse("ranking windows require ORDER BY".into()));
        }
        for expr in &spec.partition_by { expr.output_type(input.schema())?; }
        for key in &spec.order_by { key.expression.output_type(input.schema())?; }
        let mut partitions: HashMap<Vec<Value>, Vec<(usize, Vec<Value>)>> = HashMap::new();
        for row in 0..input.num_rows() {
            let group_key = spec.partition_by.iter()
                .map(|expr| expr.evaluate(input, row)).collect::<Result<Vec<_>>>()?;
            let order_key = spec.order_by.iter()
                .map(|key| key.expression.evaluate(input, row)).collect::<Result<Vec<_>>>()?;
            partitions.entry(group_key).or_default().push((row, order_key));
        }
        let mut result = vec![Value::Null; input.num_rows()];
        for mut partition in partitions.into_values() {
            partition.sort_by(|(_, left), (_, right)| compare_keys(left, right, &spec.order_by));
            let mut rank = 1usize;
            let mut dense_rank = 1usize;
            let mut last: Option<Vec<Value>> = None;
            for (position, (row, key)) in partition.into_iter().enumerate() {
                if let Some(previous) = &last {
                    if compare_keys(previous, &key, &spec.order_by) != Ordering::Equal {
                        rank = position + 1;
                        dense_rank += 1;
                    }
                }
                let number = match spec.kind {
                    WindowKind::RowNumber => position + 1,
                    WindowKind::Rank => rank,
                    WindowKind::DenseRank => dense_rank,
                };
                result[row] = Value::Integer(i64::try_from(number)
                    .map_err(|_| Error::Execution("ranking index overflow".into()))?);
                last = Some(key);
            }
        }
        let mut fields = output.schema().fields().to_vec();
        fields.push(Field::new(&spec.alias, DataType::Integer, false));
        let schema = Schema::new(fields)?;
        let mut rows = output.rows()?;
        for (row, value) in rows.iter_mut().zip(result) { row.push(value); }
        output = Batch::from_rows(schema, rows)?;
    }
    Ok(output)
}

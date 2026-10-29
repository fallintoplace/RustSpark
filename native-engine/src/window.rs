use std::{cmp::Ordering, collections::HashMap};

use crate::{
    batch::Batch, expr::Expr, scalar::Value, schema::{DataType, Field, Schema},
    sort::{compare_values, SortKey}, Error, Result,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowKind { RowNumber, Rank, DenseRank, Lag, Lead, Ntile }

#[derive(Clone, Debug, PartialEq)]
pub struct WindowSpec {
    pub kind: WindowKind,
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<SortKey>,
    pub alias: String,
    pub argument: Option<Expr>,
    pub offset: usize,
    pub default: Option<Expr>,
    pub buckets: Option<usize>,
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
        let result_type = if let Some(argument) = &spec.argument {
            let ty = argument.output_type(input.schema())?;
            if let Some(default) = &spec.default {
                let default_type = default.output_type(input.schema())?;
                if default_type != ty && !matches!(default, Expr::Literal(Value::Null)) {
                    return Err(Error::Type("LAG/LEAD default must match argument type".into()));
                }
            }
            ty
        } else { DataType::Integer };
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
            if spec.kind == WindowKind::Ntile {
                let buckets = spec.buckets.ok_or_else(||
                    Error::Execution("NTILE requires a positive bucket count".into()))?;
                if buckets == 0 {
                    return Err(Error::Execution("NTILE bucket count must be positive".into()));
                }
                let rows = partition.len();
                let base = rows / buckets;
                let extra = rows % buckets;
                let expanded = (base + 1).checked_mul(extra)
                    .ok_or_else(|| Error::Execution("NTILE bucket arithmetic overflow".into()))?;
                for (position, (row, _)) in partition.into_iter().enumerate() {
                    let bucket = if position < expanded {
                        position / (base + 1) + 1
                    } else {
                        extra + (position - expanded) / base + 1
                    };
                    result[row] = Value::Integer(i64::try_from(bucket)
                        .map_err(|_| Error::Execution("NTILE index overflow".into()))?);
                }
                continue;
            }
            if matches!(spec.kind, WindowKind::Lag | WindowKind::Lead) {
                let argument = spec.argument.as_ref().ok_or_else(||
                    Error::Execution("LAG/LEAD requires a value expression".into()))?;
                for position in 0..partition.len() {
                    let row = partition[position].0;
                    let target = match spec.kind {
                        WindowKind::Lag => position.checked_sub(spec.offset),
                        WindowKind::Lead => position.checked_add(spec.offset)
                            .filter(|next| *next < partition.len()),
                        _ => unreachable!(),
                    };
                    result[row] = if let Some(index) = target {
                        argument.evaluate(input, partition[index].0)?
                    } else if let Some(default) = &spec.default {
                        default.evaluate(input, row)?
                    } else {
                        Value::Null
                    };
                }
                continue;
            }
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
                    WindowKind::Lag | WindowKind::Lead => unreachable!("navigation handled above"),
                    WindowKind::Ntile => unreachable!("NTILE handled above"),
                };
                result[row] = Value::Integer(i64::try_from(number)
                    .map_err(|_| Error::Execution("ranking index overflow".into()))?);
                last = Some(key);
            }
        }
        let mut fields = output.schema().fields().to_vec();
        fields.push(Field::new(&spec.alias, result_type,
            matches!(spec.kind, WindowKind::Lag | WindowKind::Lead)));
        let schema = Schema::new(fields)?;
        let mut rows = output.rows()?;
        for (row, value) in rows.iter_mut().zip(result) { row.push(value); }
        output = Batch::from_rows(schema, rows)?;
    }
    Ok(output)
}

//! Partition-aware LAG and LEAD for native typed batches.
use std::cmp::Ordering;
use crate::{
    batch::Batch, expr::Expr, scalar::Value,
    schema::{Field, Schema}, sort::{compare_values, SortKey},
    Error, Result,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OffsetDirection {
    Lag,
    Lead,
}

/// Shift values within each ordered partition, preserving the original rows.
///
/// With ignore_nulls=true, offset counts only non-NULL source values.
/// A NULL source is still returned for RESPECT NULLS, while the supplied
/// default is used only when the offset falls outside its partition.
pub fn offset_batch(
    batch: Batch, partition_by: &[&str], order_by: &[SortKey],
    value_column: &str, direction: OffsetDirection, offset: usize,
    default: Value, ignore_nulls: bool, alias: &str,
) -> Result<Batch> {
    const MAX_ROWS: usize = 1_000_000;
    let len = batch.num_rows();
    if len > MAX_ROWS || offset > MAX_ROWS {
        return Err(Error::Execution("window offset or row budget exceeded".into()));
    }
    let value_index = batch.schema().column(value_column)?;
    let data_type = batch.schema().field(value_index)?.data_type;
    if !data_type.accepts(&default) {
        return Err(Error::Type("window default does not match value type".into()));
    }
    let values = batch.column(value_index)?;
    let partitions = partition_by.iter().map(|name|
        batch.column(batch.schema().column(name)?)
    ).collect::<Result<Vec<_>>>()?;
    let mut orders = Vec::new();
    for key in order_by {
        key.expression.validate_budget()?;
        let Expr::Column(name) = &key.expression else {
            return Err(Error::Unsupported("native offsets require column sort keys".into()));
        };
        orders.push(batch.column(batch.schema().column(name)?)?);
    }
    let mut indices = Vec::new();
    indices.try_reserve_exact(len)
        .map_err(|_| Error::Execution("window offset index allocation failed".into()))?;
    indices.extend(0..len);
    indices.sort_by(|&a, &b| {
        for column in &partitions {
            let order = compare_values(&column[a], &column[b], false, true);
            if order != Ordering::Equal { return order; }
        }
        for (column, key) in orders.iter().zip(order_by) {
            let order = compare_values(&column[a], &column[b],
                key.descending, key.nulls_first);
            if order != Ordering::Equal { return order; }
        }
        a.cmp(&b)
    });
    let same_partition = |a: usize, b: usize| partitions.iter().all(|column| {
        compare_values(&column[a], &column[b], false, true) == Ordering::Equal
    });
    let mut result = Vec::new();
    result.try_reserve_exact(len)
        .map_err(|_| Error::Execution("window offset output allocation failed".into()))?;
    result.resize(len, Value::Null);

    let mut start = 0usize;
    while start < len {
        let mut end = start + 1;
        while end < len && same_partition(indices[start], indices[end]) {
            end += 1;
        }
        let size = end - start;
        let mut non_null = Vec::new();
        if ignore_nulls && offset != 0 {
            non_null.try_reserve_exact(size)
                .map_err(|_| Error::Execution("window non-null index allocation failed".into()))?;
            for position in 0..size {
                if !values[indices[start + position]].is_null() {
                    non_null.push(position);
                }
            }
        }
        for position in 0..size {
            let target = if offset == 0 {
                Some(position)
            } else if ignore_nulls {
                match direction {
                    OffsetDirection::Lag => {
                        let earlier = non_null.partition_point(|&p| p < position);
                        earlier.checked_sub(offset)
                            .and_then(|index| non_null.get(index).copied())
                    }
                    OffsetDirection::Lead => {
                        let later = non_null.partition_point(|&p| p <= position);
                        later.checked_add(offset - 1)
                            .and_then(|index| non_null.get(index).copied())
                    }
                }
            } else {
                match direction {
                    OffsetDirection::Lag => position.checked_sub(offset),
                    OffsetDirection::Lead => position.checked_add(offset).filter(|&p| p < size),
                }
            };
            result[indices[start + position]] = match target {
                Some(target) => values[indices[start + target]].clone(),
                None => default.clone(),
            };
        }
        start = end;
    }
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Field::new(alias, data_type, true));
    let schema = Schema::new(fields)?;
    let mut columns = batch.into_columns();
    columns.push(result);
    Batch::from_validated_columns(schema, columns, len)
}

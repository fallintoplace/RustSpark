//! Bounded Spark-compatible NTILE ranking over native SQL batches.
use std::cmp::Ordering;

use crate::{
    batch::Batch, expr::Expr, scalar::Value,
    schema::{DataType, Field, Schema}, sort::{compare_values, SortKey},
    Error, Result,
};

/// Attach an NTILE column, preserving the input row order.
///
/// The first (partition_size % bucket_count) buckets contain one extra row.
/// Equal ORDER BY keys are assigned according to their stable input order,
/// as NTILE distributes physical rows rather than peer groups.
pub fn ntile_batch(
    batch: Batch, partition_by: &[&str], order_by: &[SortKey],
    bucket_count: usize, alias: &str,
) -> Result<Batch> {
    const MAX_ROWS: usize = 1_000_000;
    if bucket_count == 0 || bucket_count > MAX_ROWS {
        return Err(Error::Execution("NTILE bucket count must be in 1..=1000000".into()));
    }
    let len = batch.num_rows();
    if len > MAX_ROWS {
        return Err(Error::Execution("window row budget exceeded".into()));
    }
    let partitions = partition_by.iter().map(|name| {
        batch.column(batch.schema().column(name)?)
    }).collect::<Result<Vec<_>>>()?;
    let mut orders = Vec::new();
    for key in order_by {
        key.expression.validate_budget()?;
        let Expr::Column(name) = &key.expression else {
            return Err(Error::Unsupported("native NTILE requires column sort keys".into()));
        };
        orders.push(batch.column(batch.schema().column(name)?)?);
    }
    let mut indices = Vec::new();
    indices.try_reserve_exact(len)
        .map_err(|_| Error::Execution("NTILE row index allocation failed".into()))?;
    indices.extend(0..len);
    indices.sort_by(|&a, &b| {
        for column in &partitions {
            let ordering = compare_values(&column[a], &column[b], false, true);
            if ordering != Ordering::Equal { return ordering; }
        }
        for (column, key) in orders.iter().zip(order_by) {
            let ordering = compare_values(&column[a], &column[b],
                key.descending, key.nulls_first);
            if ordering != Ordering::Equal { return ordering; }
        }
        a.cmp(&b)
    });
    let same_partition = |a: usize, b: usize| partitions.iter().all(|column| {
        compare_values(&column[a], &column[b], false, true) == Ordering::Equal
    });
    let mut values = Vec::new();
    values.try_reserve_exact(len)
        .map_err(|_| Error::Execution("NTILE result allocation failed".into()))?;
    values.resize(len, Value::Null);

    let mut start = 0usize;
    while start < len {
        let mut end = start + 1;
        while end < len && same_partition(indices[start], indices[end]) {
            end += 1;
        }
        let count = end - start;
        let smaller = count / bucket_count;
        let larger_buckets = count % bucket_count;
        // All first larger_buckets get one additional row. Multiplication
        // cannot overflow because larger_buckets <= count <= MAX_ROWS.
        let larger_rows = larger_buckets * (smaller + 1);
        for (position, &row) in indices[start..end].iter().enumerate() {
            let bucket = if position < larger_rows {
                position / (smaller + 1) + 1
            } else {
                // Here smaller > 0, because if n > count all rows are
                // assigned to the first n buckets.
                larger_buckets + (position - larger_rows) / smaller + 1
            };
            values[row] = Value::Integer(bucket as i64);
        }
        start = end;
    }
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Field::new(alias, DataType::Integer, false));
    let schema = Schema::new(fields)?;
    let mut columns = batch.into_columns();
    columns.push(values);
    Batch::from_validated_columns(schema, columns, len)
}

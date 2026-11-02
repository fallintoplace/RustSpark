//! Native partitioned SQL ranking primitives. SQL OVER lowering can reuse
//! these without changing a batch's input row order.
use std::cmp::Ordering;

use crate::{
    batch::Batch, expr::Expr, scalar::Value,
    schema::{DataType, Field, Schema}, sort::{compare_values, SortKey},
    Error, Result,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ranking {
    RowNumber,
    Rank,
    DenseRank,
}

/// Compute one ranking column over a typed native batch. Results are returned
/// in the original row order, even though partition/order keys are sorted in
/// a temporary row-index vector.
pub fn rank_batch(
    batch: Batch, partition_by: &[&str], order_by: &[SortKey],
    ranking: Ranking, alias: &str,
) -> Result<Batch> {
    const MAX_WINDOW_ROWS: usize = 1_000_000;
    let rows = batch.num_rows();
    if rows > MAX_WINDOW_ROWS {
        return Err(Error::Execution("window row budget exceeded".into()));
    }
    let partition_columns = partition_by.iter()
        .map(|name| batch.column(batch.schema().column(name)?))
        .collect::<Result<Vec<_>>>()?;
    let mut order_columns = Vec::new();
    for key in order_by {
        key.expression.validate_budget()?;
        let Expr::Column(name) = &key.expression else {
            return Err(Error::Unsupported("native ranking requires column sort keys".into()));
        };
        order_columns.push(batch.column(batch.schema().column(name)?)?);
    }
    let mut indices = Vec::new();
    indices.try_reserve_exact(rows)
        .map_err(|_| Error::Execution("window index allocation failed".into()))?;
    indices.extend(0..rows);
    indices.sort_by(|&a, &b| {
        for column in &partition_columns {
            let cmp = compare_values(&column[a], &column[b], false, true);
            if cmp != Ordering::Equal { return cmp; }
        }
        for (column, key) in order_columns.iter().zip(order_by) {
            let cmp = compare_values(
                &column[a], &column[b], key.descending, key.nulls_first);
            if cmp != Ordering::Equal { return cmp; }
        }
        a.cmp(&b)
    });
    let same_partition = |left: usize, right: usize| {
        partition_columns.iter().all(|column| column[left] == column[right])
    };
    let same_order = |left: usize, right: usize| {
        order_columns.iter().zip(order_by).all(|(column, key)|
            compare_values(&column[left], &column[right],
                key.descending, key.nulls_first) == Ordering::Equal)
    };
    let mut result = Vec::new();
    result.try_reserve_exact(rows)
        .map_err(|_| Error::Execution("window rank allocation failed".into()))?;
    result.resize(rows, Value::Null);
    let mut partition_position = 0usize;
    let mut rank = 0usize;
    let mut dense_rank = 0usize;
    let mut previous: Option<usize> = None;
    for &row in &indices {
        let new_partition = previous.is_none_or(|old| !same_partition(old, row));
        if new_partition {
            partition_position = 1;
            rank = 1;
            dense_rank = 1;
        } else {
            partition_position += 1;
            if previous.is_some_and(|old| !same_order(old, row)) {
                rank = partition_position;
                dense_rank += 1;
            }
        }
        let ordinal = match ranking {
            Ranking::RowNumber => partition_position,
            Ranking::Rank => rank,
            Ranking::DenseRank => dense_rank,
        };
        result[row] = Value::Integer(i64::try_from(ordinal)
            .map_err(|_| Error::Execution("window rank overflow".into()))?);
        previous = Some(row);
    }
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Field::new(alias, DataType::Integer, false));
    let schema = Schema::new(fields)?;
    let mut columns = batch.into_columns();
    columns.push(result);
    Batch::from_validated_columns(schema, columns, rows)
}

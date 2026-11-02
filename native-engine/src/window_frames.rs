//! ROWS-framed numeric SQL aggregates over typed native batches.
use std::cmp::Ordering;
use crate::{
    batch::Batch, expr::Expr, scalar::Value,
    schema::{DataType, Field, Schema}, sort::{compare_values, SortKey},
    Error, Result,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameAggregate {
    Sum,
    Average,
    Count,
}

#[derive(Default)]
struct Accumulator {
    count: usize,
    integer_sum: i128,
    float_sum: f64,
}

impl Accumulator {
    fn adjust(&mut self, value: &Value, data_type: DataType, add: bool) -> Result<()> {
        if value.is_null() { return Ok(()); }
        if add { self.count += 1; }
        else { self.count -= 1; }
        match (value, data_type) {
            (Value::Integer(value), DataType::Integer) => {
                if add { self.integer_sum += i128::from(*value); }
                else { self.integer_sum -= i128::from(*value); }
            }
            (Value::Float(value), DataType::Float) => {
                if add { self.float_sum += *value; }
                else { self.float_sum -= *value; }
            }
            // COUNT can operate on nonnumeric values. The caller rejects
            // nonnumeric types for SUM and AVG before execution.
            _ => {}
        }
        Ok(())
    }

    fn result(&self, data_type: DataType, aggregate: FrameAggregate) -> Result<Value> {
        match aggregate {
            FrameAggregate::Count => Ok(Value::Integer(
                i64::try_from(self.count)
                    .map_err(|_| Error::Execution("window count overflow".into()))?
            )),
            FrameAggregate::Sum | FrameAggregate::Average if self.count == 0 => {
                Ok(Value::Null)
            }
            FrameAggregate::Sum => match data_type {
                DataType::Integer => Ok(Value::Integer(
                    i64::try_from(self.integer_sum)
                        .map_err(|_| Error::Execution("window integer sum overflow".into()))?
                )),
                DataType::Float => Ok(Value::Float(self.float_sum)),
                _ => unreachable!("numeric SUM was validated"),
            },
            FrameAggregate::Average => {
                let sum = match data_type {
                    DataType::Integer => self.integer_sum as f64,
                    DataType::Float => self.float_sum,
                    _ => unreachable!("numeric AVG was validated"),
                };
                Ok(Value::Float(sum / self.count as f64))
            }
        }
    }
}

/// Calculate SUM, AVG or COUNT for ROWS BETWEEN N PRECEDING AND M FOLLOWING.
///
/// The output is aligned to the input row order. Ties use stable input order.
/// Each sorted partition uses a sliding accumulator in linear time rather
/// than rescanning every frame, including when the frame covers many rows.
pub fn aggregate_rows_frame(
    batch: Batch, partition_by: &[&str], order_by: &[SortKey],
    value_column: &str, preceding: usize, following: usize,
    aggregate: FrameAggregate, alias: &str,
) -> Result<Batch> {
    const MAX_ROWS: usize = 1_000_000;
    let len = batch.num_rows();
    if len > MAX_ROWS {
        return Err(Error::Execution("window row budget exceeded".into()));
    }
    let value_index = batch.schema().column(value_column)?;
    let data_type = batch.schema().field(value_index)?.data_type;
    if aggregate != FrameAggregate::Count
        && !matches!(data_type, DataType::Integer | DataType::Float) {
        return Err(Error::Type("window SUM and AVG need numeric input".into()));
    }
    let output_type = match aggregate {
        FrameAggregate::Count => DataType::Integer,
        FrameAggregate::Sum => data_type,
        FrameAggregate::Average => DataType::Float,
    };
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Field::new(alias, output_type, aggregate != FrameAggregate::Count));
    let output_schema = Schema::new(fields)?;

    let values = batch.column(value_index)?;
    let partitions = partition_by.iter().map(|name|
        batch.column(batch.schema().column(name)?)
    ).collect::<Result<Vec<_>>>()?;
    let mut orders = Vec::new();
    for key in order_by {
        key.expression.validate_budget()?;
        let Expr::Column(name) = &key.expression else {
            return Err(Error::Unsupported("native frame aggregate requires column sort keys".into()));
        };
        orders.push(batch.column(batch.schema().column(name)?)?);
    }
    let mut indices = Vec::new();
    indices.try_reserve_exact(len)
        .map_err(|_| Error::Execution("window row index allocation failed".into()))?;
    indices.extend(0..len);
    indices.sort_by(|&left, &right| {
        for column in &partitions {
            let comparison = compare_values(&column[left], &column[right], false, true);
            if comparison != Ordering::Equal { return comparison; }
        }
        for (column, key) in orders.iter().zip(order_by) {
            let comparison = compare_values(&column[left], &column[right],
                key.descending, key.nulls_first);
            if comparison != Ordering::Equal { return comparison; }
        }
        left.cmp(&right)
    });
    let same_partition = |left: usize, right: usize| partitions.iter().all(|column| {
        compare_values(&column[left], &column[right], false, true) == Ordering::Equal
    });
    let mut output = Vec::new();
    output.try_reserve_exact(len)
        .map_err(|_| Error::Execution("window frame result allocation failed".into()))?;
    output.resize(len, Value::Null);

    let mut begin = 0usize;
    while begin < len {
        let mut end = begin + 1;
        while end < len && same_partition(indices[begin], indices[end]) {
            end += 1;
        }
        let size = end - begin;
        let mut active = Accumulator::default();
        let mut active_start = 0usize;
        let mut active_end = 0usize;
        for position in 0..size {
            let frame_start = position.saturating_sub(preceding);
            let frame_end = position.saturating_add(following)
                .saturating_add(1).min(size);
            // Remove old values first so the temporary accumulator never
            // contains values outside the union of adjacent frames.
            while active_start < frame_start {
                active.adjust(&values[indices[begin + active_start]], data_type, false)?;
                active_start += 1;
            }
            while active_end < frame_end {
                active.adjust(&values[indices[begin + active_end]], data_type, true)?;
                active_end += 1;
            }
            output[indices[begin + position]] = active.result(data_type, aggregate)?;
        }
        begin = end;
    }
    let mut columns = batch.into_columns();
    columns.push(output);
    Batch::from_validated_columns(output_schema, columns, len)
}

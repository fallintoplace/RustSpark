//! Hash-partitioned grouped aggregation with bounded per-shard states.
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use crate::{
    aggregate::{AggregateItem, AggregateOp},
    batch::Batch,
    expr::Expr,
    scalar::Value,
    schema::{DataType, Field, Schema},
    Error, Result,
};
use super::{
    cancel::QueryControl, chunks::BatchChunks, partition_spill::PartitionWriter,
    spill::{SpillReader, SpillWriter},
};

static GROUP_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct GroupFiles {
    paths: Vec<PathBuf>,
    schema: Schema,
}

impl GroupFiles {
    pub fn paths(&self) -> &[PathBuf] { &self.paths }
    pub fn schema(&self) -> &Schema { &self.schema }
}

impl Drop for GroupFiles {
    fn drop(&mut self) {
        for path in &self.paths { let _ = std::fs::remove_file(path); }
    }
}

enum State {
    Count(i64),
    Sum(Option<Value>),
    Avg { sum: f64, count: i64 },
    Min(Option<Value>),
    Max(Option<Value>),
}

fn new_state(op: &AggregateOp) -> Result<State> {
    match op {
        AggregateOp::CountAll | AggregateOp::Count(_) => Ok(State::Count(0)),
        AggregateOp::Sum(_) => Ok(State::Sum(None)),
        AggregateOp::Min(_) => Ok(State::Min(None)),
        AggregateOp::Max(_) => Ok(State::Max(None)),
        AggregateOp::Avg(_) => Ok(State::Avg { sum: 0.0, count: 0 }),
        AggregateOp::CountDistinct(_) => Err(Error::Unsupported("spill COUNT DISTINCT is not implemented".into())),
    }
}

fn add_to_state(state: &mut State, op: &AggregateOp, batch: &Batch, row: usize) -> Result<()> {
    let value = match op {
        AggregateOp::CountAll => Value::Integer(1),
        AggregateOp::Count(expr) | AggregateOp::Sum(expr) |
        AggregateOp::Avg(expr) | AggregateOp::Min(expr) |
        AggregateOp::Max(expr) => expr.evaluate(batch, row)?,
        AggregateOp::CountDistinct(_) => return Err(Error::Unsupported("spill COUNT DISTINCT is not implemented".into())),
    };
    if value.is_null() { return Ok(()); }
    let is_min = matches!(state, State::Min(_));
    match state {
        State::Count(count) => {
            *count = count.checked_add(1)
                .ok_or_else(|| Error::Execution("COUNT overflow".into()))?;
        }
        State::Sum(total) => {
            let next = match (total.as_ref(), &value) {
                (None, Value::Integer(_) | Value::Float(_)) => value.clone(),
                (Some(Value::Integer(a)), Value::Integer(b)) => Value::Integer(
                    a.checked_add(*b).ok_or_else(|| Error::Execution("SUM overflow".into()))?),
                (Some(Value::Float(a)), Value::Float(b)) => Value::Float(*a + *b),
                (Some(Value::Float(a)), Value::Integer(b)) => Value::Float(*a + *b as f64),
                (Some(Value::Integer(a)), Value::Float(b)) => Value::Float(*a as f64 + *b),
                _ => return Err(Error::Type("SUM requires numeric values".into())),
            };
            if matches!(next, Value::Float(v) if !v.is_finite()) {
                return Err(Error::Execution("SUM overflow".into()));
            }
            *total = Some(next);
        }
        State::Avg { sum, count } => {
            let number = match value {
                Value::Integer(number) => number as f64,
                Value::Float(number) => number,
                _ => return Err(Error::Type("AVG requires numeric values".into())),
            };
            let next = *sum + number;
            if !next.is_finite() { return Err(Error::Execution("AVG overflow".into())); }
            let next_count = count.checked_add(1)
                .ok_or_else(|| Error::Execution("AVG count overflow".into()))?;
            *sum = next;
            *count = next_count;
        }
        State::Min(current) | State::Max(current) => {
            if let Some(existing) = current {
                let order = value.sql_compare(existing)?
                    .ok_or_else(|| Error::Type("non-comparable MIN/MAX input".into()))?;
                if (is_min && order.is_lt()) || (!is_min && order.is_gt()) {
                    *existing = value;
                }
            } else { *current = Some(value); }
        }
    }
    Ok(())
}

fn output_schema(input: &Schema, group_names: &[(String, String)], items: &[AggregateItem]) -> Result<Schema> {
    let mut fields = Vec::new();
    for (column, alias) in group_names {
        let field = input.field(input.column(column)?)?;
        fields.push(Field::new(alias, field.data_type, true));
    }
    for item in items {
        let ty = match &item.op {
            AggregateOp::CountAll | AggregateOp::Count(_) => DataType::Integer,
            AggregateOp::Sum(expr) => match expr.output_type(input)? {
                DataType::Integer => DataType::Integer,
                DataType::Float => DataType::Float,
                _ => return Err(Error::Type("SUM requires numeric input".into())),
            },
            AggregateOp::Min(expr) | AggregateOp::Max(expr) => expr.output_type(input)?,
            AggregateOp::Avg(expr) => match expr.output_type(input)? {
                DataType::Integer | DataType::Float => DataType::Float,
                _ => return Err(Error::Type("AVG requires numeric input".into())),
            },
            AggregateOp::CountDistinct(_) => return Err(Error::Unsupported("spill COUNT DISTINCT is not implemented".into())),
        };
        let nullable = !matches!(&item.op, AggregateOp::CountAll | AggregateOp::Count(_));
        fields.push(Field::new(&item.alias, ty, nullable));
    }
    Schema::new(fields)
}

fn aggregate_shard(
    path: &Path, input: &Schema, keys: &[usize],
    items: &[AggregateItem], output: &Schema,
    max_groups: usize, io_rows: usize, control: &QueryControl,
) -> Result<Batch> {
    let mut reader = SpillReader::new(BufReader::new(File::open(path)?), io_rows)?;
    if reader.schema() != input { return Err(Error::Schema("group shard schema mismatch".into())); }
    let mut groups: HashMap<Vec<Value>, Vec<State>> = HashMap::new();
    while let Some(batch) = reader.next_batch()? {
        control.check()?;
        for row in 0..batch.num_rows() {
            let key = keys.iter().map(|&col| batch.value(row, col).cloned())
                .collect::<Result<Vec<_>>>()?;
            if !groups.contains_key(&key) && groups.len() >= max_groups {
                return Err(Error::Execution("group state count exceeds partition budget".into()));
            }
            let states = match groups.entry(key) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let initial = items.iter()
                        .map(|item| new_state(&item.op))
                        .collect::<Result<Vec<_>>>()?;
                    entry.insert(initial)
                }
            };
            for (state, item) in states.iter_mut().zip(items) {
                add_to_state(state, &item.op, &batch, row)?;
            }
        }
    }
    let rows = groups.into_iter().map(|(mut key, states)| {
        for state in states {
            key.push(match state {
                State::Count(value) => Value::Integer(value),
                State::Avg { sum, count } => if count == 0 {
                    Value::Null
                } else {
                    Value::Float(sum / count as f64)
                },
                State::Sum(v) | State::Min(v) | State::Max(v) => v.unwrap_or(Value::Null),
            });
        }
        key
    }).collect();
    Batch::from_rows(output.clone(), rows)
}

#[allow(clippy::too_many_arguments)]
pub fn grouped_aggregate<I>(
    input: I, schema: Schema, groups: &[(String, String)],
    items: &[AggregateItem], directory: &Path,
    partitions: usize, max_groups_per_partition: usize,
    io_batch_rows: usize, control: &QueryControl,
) -> Result<GroupFiles>
where I: IntoIterator<Item = Result<Batch>>
{
    if groups.is_empty() {
        return Err(Error::Unsupported("grouped spill requires GROUP BY columns".into()));
    }
    if max_groups_per_partition == 0 || io_batch_rows == 0 {
        return Err(Error::Execution("invalid grouped aggregation memory limits".into()));
    }
    let keys = groups.iter().map(|(name, _)| schema.column(name))
        .collect::<Result<Vec<_>>>()?;
    let output_schema = output_schema(&schema, groups, items)?;
    let names = groups.iter().map(|(name, _)| name.as_str()).collect::<Vec<_>>();
    let mut writer = PartitionWriter::new(
        directory, schema.clone(), &names, partitions, io_batch_rows)?;
    for incoming in input {
        control.check()?;
        writer.write_batch(&incoming?, control)?;
    }
    let files = writer.finish()?;
    let mut result = GroupFiles { paths: Vec::new(), schema: output_schema.clone() };
    for shard in files.paths() {
        control.check()?;
        let batch = aggregate_shard(
            shard, &schema, &keys, items, &output_schema,
            max_groups_per_partition, io_batch_rows, control)?;
        let id = GROUP_ID.fetch_add(1, Ordering::Relaxed);
        let path = directory.join(format!("rustspark-groups-{}-{id}.rspill", std::process::id()));
        let file = OpenOptions::new().write(true).create_new(true).open(&path)?;
        result.paths.push(path);
        let mut output_writer = SpillWriter::new(BufWriter::new(file), output_schema.clone())?;
        for piece in BatchChunks::new(&batch, io_batch_rows)? {
            output_writer.write_batch(&piece?)?;
        }
        output_writer.finish()?;
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{schema::{DataType, Field}, expr::Expr};
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn native_spill_avg_across_batches_and_null_values() {
        let dir = std::env::temp_dir().join(format!(
            "native-spill-avg-{}-{}", std::process::id(), TEST_ID.fetch_add(1, AtomicOrdering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![
            Field::new("g", DataType::Integer, false),
            Field::new("n", DataType::Integer, true),
        ]).unwrap();
        let a = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1), Value::Integer(2)],
            vec![Value::Integer(2), Value::Null],
        ]).unwrap();
        let b = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1), Value::Integer(6)],
            vec![Value::Integer(1), Value::Null],
        ]).unwrap();
        let result = grouped_aggregate([Ok(a), Ok(b)], schema,
            &[("g".into(), "g".into())],
            &[AggregateItem::new(AggregateOp::Avg(Expr::Column("n".into())), "avg")],
            &dir, 4, 10, 2, &QueryControl::default()).unwrap();
        let mut rows = Vec::new();
        for path in result.paths() {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 2).unwrap();
            while let Some(batch) = reader.next_batch().unwrap() { rows.extend(batch.rows().unwrap()); }
        }
        assert!(rows.contains(&vec![Value::Integer(1), Value::Float(4.0)]));
        assert!(rows.contains(&vec![Value::Integer(2), Value::Null]));
        drop(result);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn native_spill_rejects_unsupported_count_distinct() {
        let schema = Schema::new(vec![
            Field::new("g", DataType::Integer, false),
            Field::new("n", DataType::Integer, true),
        ]).unwrap();
        let error = grouped_aggregate(
            Vec::<Result<Batch>>::new(), schema,
            &[("g".into(), "g".into())],
            &[AggregateItem::new(AggregateOp::CountDistinct(Expr::Column("n".into())), "unique")],
            Path::new("."), 2, 10, 2, &QueryControl::default(),
        ).unwrap_err();
        assert!(error.to_string().contains("spill COUNT DISTINCT is not implemented"));
    }

    #[test]
    fn group_counts_and_sums_cross_input_batch_boundaries() {
        let dir = std::env::temp_dir().join(format!("native-groups-{}-{}",
            std::process::id(), TEST_ID.fetch_add(1, AtomicOrdering::Relaxed)));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![
            Field::new("g", DataType::Integer, false),
            Field::new("n", DataType::Integer, true)
        ]).unwrap();
        let one = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1), Value::Integer(3)],
            vec![Value::Integer(2), Value::Integer(9)]
        ]).unwrap();
        let two = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1), Value::Integer(5)],
            vec![Value::Integer(2), Value::Null]
        ]).unwrap();
        let items = vec![
            AggregateItem::new(AggregateOp::CountAll, "n_rows"),
            AggregateItem::new(AggregateOp::Sum(Expr::Column("n".into())), "total")
        ];
        let result = grouped_aggregate([Ok(one), Ok(two)], schema,
            &[("g".into(), "g".into())], &items, &dir, 4, 10, 2,
            &QueryControl::default()).unwrap();
        let mut all = Vec::new();
        for path in result.paths() {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 2).unwrap();
            while let Some(batch) = reader.next_batch().unwrap() { all.extend(batch.rows().unwrap()); }
        }
        assert_eq!(all.len(), 2);
        assert!(all.contains(&vec![Value::Integer(1), Value::Integer(2), Value::Integer(8)]));
        assert!(all.contains(&vec![Value::Integer(2), Value::Integer(2), Value::Integer(9)]));
        drop(result);
        fs::remove_dir(dir).unwrap();
    }
}

#[cfg(test)]
mod spill_count_schema_tests {
    use super::*;

    #[test]
    fn only_null_producing_aggregates_are_nullable() {
        let input = Schema::new(vec![
            Field::new("key", DataType::Integer, false),
            Field::new("amount", DataType::Integer, true),
        ]).unwrap();
        let result = output_schema(&input, &[("key".into(), "key".into())], &[
            AggregateItem::new(AggregateOp::CountAll, "count_all"),
            AggregateItem::new(AggregateOp::Count(Expr::Column("amount".into())), "count_valid"),
            AggregateItem::new(AggregateOp::Sum(Expr::Column("amount".into())), "total"),
        ]).unwrap();
        assert!(!result.field(1).unwrap().nullable);
        assert!(!result.field(2).unwrap().nullable);
        assert!(result.field(3).unwrap().nullable);
    }
}

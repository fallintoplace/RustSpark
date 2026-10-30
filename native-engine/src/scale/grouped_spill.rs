//! Hash-partitioned grouped aggregation with bounded per-shard states.
use std::{
    collections::{HashMap, HashSet},
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
    cancel::QueryControl, partition_spill::PartitionWriter,
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

const MAX_DISTINCT_KEYS_PER_GROUP: usize = 65_536;
const MAX_DISTINCT_BYTES_PER_GROUP: usize = 16 * 1024 * 1024;

enum State {
    Count(i64),
    CountDistinct { keys: HashSet<Value>, bytes: usize },
    Sum(Option<Value>),
    SumDistinct { keys: HashSet<Value>, bytes: usize, total: Option<Value> },
    Avg { sum: f64, count: i64 },
    AvgDistinct { keys: HashSet<Value>, bytes: usize, sum: f64, count: i64 },
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
        AggregateOp::CountDistinct(_) => Ok(State::CountDistinct { keys: HashSet::new(), bytes: 0 }),
        AggregateOp::SumDistinct(_) => Ok(State::SumDistinct {
            keys: HashSet::new(), bytes: 0, total: None,
        }),
        AggregateOp::AvgDistinct(_) => Ok(State::AvgDistinct {
            keys: HashSet::new(), bytes: 0, sum: 0.0, count: 0,
        }),
    }
}

/// Distinct state is bounded by both key count and estimated memory bytes.
fn insert_distinct(
    keys: &mut HashSet<Value>, bytes: &mut usize, value: &Value
) -> Result<bool> {
    if keys.contains(value) { return Ok(false); }
    if keys.len() >= MAX_DISTINCT_KEYS_PER_GROUP {
        return Err(Error::Execution("spill DISTINCT key limit exceeded".into()));
    }
    let footprint = std::mem::size_of::<Value>().saturating_add(64)
        .saturating_add(match value { Value::Text(text) => text.len(), _ => 0 });
    let next = bytes.checked_add(footprint)
        .filter(|size| *size <= MAX_DISTINCT_BYTES_PER_GROUP)
        .ok_or_else(|| Error::Execution("spill DISTINCT byte limit exceeded".into()))?;
    keys.insert(value.clone());
    *bytes = next;
    Ok(true)
}

fn add_sum(total: &mut Option<Value>, value: &Value) -> Result<()> {
    let next = match (total.as_ref(), value) {
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
    Ok(())
}

fn add_avg(sum: &mut f64, count: &mut i64, value: &Value) -> Result<()> {
    let number = match value {
        Value::Integer(number) => *number as f64,
        Value::Float(number) => *number,
        _ => return Err(Error::Type("AVG requires numeric values".into())),
    };
    let next = *sum + number;
    if !next.is_finite() { return Err(Error::Execution("AVG overflow".into())); }
    let next_count = count.checked_add(1)
        .ok_or_else(|| Error::Execution("AVG count overflow".into()))?;
    *sum = next;
    *count = next_count;
    Ok(())
}

fn add_to_state(state: &mut State, op: &AggregateOp, batch: &Batch, row: usize) -> Result<()> {
    let value = match op {
        AggregateOp::CountAll => Value::Integer(1),
        AggregateOp::Count(expr) | AggregateOp::CountDistinct(expr) | AggregateOp::Sum(expr) |
        AggregateOp::Avg(expr) | AggregateOp::Min(expr) |
        AggregateOp::Max(expr) | AggregateOp::SumDistinct(expr) |
        AggregateOp::AvgDistinct(expr) => expr.evaluate(batch, row)?,
    };
    if value.is_null() { return Ok(()); }
    let is_min = matches!(state, State::Min(_));
    match state {
        State::Count(count) => {
            *count = count.checked_add(1)
                .ok_or_else(|| Error::Execution("COUNT overflow".into()))?;
        }
        State::CountDistinct { keys, bytes } => {
            insert_distinct(keys, bytes, &value)?;
        }
        State::Sum(total) => add_sum(total, &value)?,
        State::SumDistinct { keys, bytes, total } => {
            if insert_distinct(keys, bytes, &value)? { add_sum(total, &value)?; }
        }
        State::Avg { sum, count } => add_avg(sum, count, &value)?,
        State::AvgDistinct { keys, bytes, sum, count } => {
            if insert_distinct(keys, bytes, &value)? { add_avg(sum, count, &value)?; }
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
            AggregateOp::CountAll | AggregateOp::Count(_) | AggregateOp::CountDistinct(_) => DataType::Integer,
            AggregateOp::Sum(expr) | AggregateOp::SumDistinct(expr) =>
                match expr.output_type(input)? {
                DataType::Integer => DataType::Integer,
                DataType::Float => DataType::Float,
                _ => return Err(Error::Type("SUM requires numeric input".into())),
            },
            AggregateOp::Min(expr) | AggregateOp::Max(expr) => expr.output_type(input)?,
            AggregateOp::Avg(expr) | AggregateOp::AvgDistinct(expr) => match expr.output_type(input)? {
                DataType::Integer | DataType::Float => DataType::Float,
                _ => return Err(Error::Type("AVG requires numeric input".into())),
            },
        };
        let nullable = !matches!(&item.op, AggregateOp::CountAll | AggregateOp::Count(_) | AggregateOp::CountDistinct(_));
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
            let at_capacity = groups.len() >= max_groups;
            let states = match groups.entry(key) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    if at_capacity {
                        return Err(Error::Execution("group state count exceeds partition budget".into()));
                    }
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
                State::CountDistinct { keys, .. } => Value::Integer(keys.len() as i64),
                State::AvgDistinct { sum, count, .. } | State::Avg { sum, count } =>
                    if count == 0 {
                    Value::Null
                } else {
                    Value::Float(sum / count as f64)
                },
                State::SumDistinct { total: v, .. } |
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
        let file = open_private_spill(&path)?;
        result.paths.push(path);
        let mut output_writer = SpillWriter::new(BufWriter::new(file), output_schema.clone())?;
        let mut start = 0;
        while start < batch.num_rows() {
            let end = start.saturating_add(io_batch_rows).min(batch.num_rows());
            output_writer.write_batch_range(&batch, start, end)?;
            start = end;
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
    fn grouped_spill_count_distinct_ignores_nulls_and_duplicates() {
        let dir = std::env::temp_dir().join(format!("native-count-distinct-{}-{}",
            std::process::id(), TEST_ID.fetch_add(1, AtomicOrdering::Relaxed)));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![
            Field::new("g", DataType::Integer, false),
            Field::new("n", DataType::Integer, true),
        ]).unwrap();
        let input = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1), Value::Integer(2)],
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(1), Value::Integer(2)],
            vec![Value::Integer(1), Value::Integer(3)],
            vec![Value::Integer(2), Value::Null],
        ]).unwrap();
        let result = grouped_aggregate([Ok(input)], schema,
            &[("g".into(), "g".into())],
            &[AggregateItem::new(AggregateOp::CountDistinct(Expr::Column("n".into())), "unique")],
            &dir, 4, 10, 2, &QueryControl::default()).unwrap();
        let mut rows = Vec::new();
        for path in result.paths() {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 2).unwrap();
            while let Some(batch) = reader.next_batch().unwrap() {
                assert!(!batch.schema().field(1).unwrap().nullable);
                rows.extend(batch.rows().unwrap());
            }
        }
        assert!(rows.contains(&vec![Value::Integer(1), Value::Integer(2)]));
        assert!(rows.contains(&vec![Value::Integer(2), Value::Integer(0)]));
        drop(result);
        fs::remove_dir(dir).unwrap();
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

#[cfg(test)]
mod grouped_spill_window_output_tests {
    use super::*;
    use crate::schema::{DataType, Field};
    use std::{fs, sync::atomic::{AtomicU64, Ordering as AtomicOrdering}};
    static ID: AtomicU64 = AtomicU64::new(1);
    #[test]
    fn grouped_output_can_be_read_in_bounded_windows() {
        let dir = std::env::temp_dir().join(format!("native-group-windows-{}-{}",
            std::process::id(), ID.fetch_add(1, AtomicOrdering::Relaxed)));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("k", DataType::Integer, false)]).unwrap();
        let input = Batch::from_rows(schema.clone(), (0..11).map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let output = grouped_aggregate([Ok(input)], schema, &[(String::from("k"), String::from("k"))],
            &[AggregateItem::new(AggregateOp::CountAll, "cnt")], &dir, 1, 20, 2,
            &QueryControl::default()).unwrap();
        let mut reader = SpillReader::new(BufReader::new(File::open(&output.paths()[0]).unwrap()), 2).unwrap();
        let mut rows = Vec::new();
        while let Some(batch) = reader.next_batch().unwrap() {
            assert!(batch.num_rows() <= 2);
            rows.extend(batch.rows().unwrap());
        }
        assert_eq!(rows.len(), 11);
        assert!(rows.iter().all(|row| row[1] == Value::Integer(1)));
        drop(output);
        fs::remove_dir(dir).unwrap();
    }
}

fn open_private_spill(path: &std::path::Path) -> Result<std::fs::File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    Ok(options.open(path)?)
}

#[cfg(all(test, unix))]
mod private_spill_permissions_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn scratch_files_are_not_world_readable() {
        let path = std::env::temp_dir().join(format!(
            "rustspark-private-grouped_spill-{}-{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let file = open_private_spill(&path).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o077, 0);
        drop(file);
        std::fs::remove_file(path).unwrap();
    }
}

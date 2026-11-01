//! Hash-partitioned grouped aggregation with bounded per-shard states.
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufReader, BufWriter},
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use crate::{
    aggregate::{AggregateItem, AggregateOp},
    batch::Batch,
    scalar::Value,
    schema::{DataType, Field, Schema},
    Error, Result,
};
use super::{
    cancel::QueryControl, partition_spill::PartitionWriter,
    spill::{SpillReader, SpillWriter},
};

use crate::expr::Expr;

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
const MAX_GROUP_KEY_STATE_BYTES: usize = 64 * 1024 * 1024;

enum State {
    Count(i64),
    CountDistinct { keys: HashSet<Value>, bytes: usize },
    Sum(Option<Value>),
    SumDistinct { keys: HashSet<Value>, bytes: usize, total: Option<Value> },
    Avg { sum: f64, compensation: f64, count: i64 },
    AvgDistinct { keys: HashSet<Value>, bytes: usize, sum: f64, compensation: f64, count: i64 },
    Min(Option<Value>),
    Max(Option<Value>),
}

fn new_state(op: &AggregateOp) -> Result<State> {
    match op {
        AggregateOp::CountAll | AggregateOp::Count(_) => Ok(State::Count(0)),
        AggregateOp::Sum(_) => Ok(State::Sum(None)),
        AggregateOp::Min(_) => Ok(State::Min(None)),
        AggregateOp::Max(_) => Ok(State::Max(None)),
        AggregateOp::Avg(_) => Ok(State::Avg { sum: 0.0, compensation: 0.0, count: 0 }),
        AggregateOp::CountDistinct(_) => Ok(State::CountDistinct { keys: HashSet::new(), bytes: 0 }),
        AggregateOp::SumDistinct(_) => Ok(State::SumDistinct {
            keys: HashSet::new(), bytes: 0, total: None,
        }),
        AggregateOp::AvgDistinct(_) => Ok(State::AvgDistinct {
            keys: HashSet::new(), bytes: 0, sum: 0.0, compensation: 0.0, count: 0,
        }),
    }
}

/// Distinct state is bounded by both key count and estimated memory bytes.
fn insert_distinct(
    keys: &mut HashSet<Value>, bytes: &mut usize, value: &Value,
    retained_bytes: &mut usize, max_retained_bytes: usize,
) -> Result<bool> {
    if keys.contains(value) { return Ok(false); }
    if keys.len() >= MAX_DISTINCT_KEYS_PER_GROUP {
        return Err(Error::Execution("spill DISTINCT key limit exceeded".into()));
    }
    // Source strings may retain a large unused allocation. The distinct
    // table owns a fresh clone, so admit its minimum possible size first.
    let minimum = super::row_footprint::minimum_cloned_footprint(std::iter::once(value));
    bytes.checked_add(minimum)
        .filter(|size| *size <= MAX_DISTINCT_BYTES_PER_GROUP)
        .ok_or_else(|| Error::Execution("spill DISTINCT byte limit exceeded".into()))?;
    retained_bytes.checked_add(minimum)
        .filter(|size| *size <= max_retained_bytes)
        .ok_or_else(|| Error::Execution("spill DISTINCT partition byte budget exceeded".into()))?;

    let owned = value.clone();
    let footprint = super::row_footprint::footprint(std::iter::once(&owned));
    let next = bytes.checked_add(footprint)
        .filter(|size| *size <= MAX_DISTINCT_BYTES_PER_GROUP)
        .ok_or_else(|| Error::Execution("spill DISTINCT byte limit exceeded".into()))?;
    // Per-group limits do not bound total distinct state across thousands
    // of groups. Share the shard's existing key/state admission budget.
    let total = retained_bytes.checked_add(footprint)
        .filter(|size| *size <= max_retained_bytes)
        .ok_or_else(|| Error::Execution("spill DISTINCT partition byte budget exceeded".into()))?;
    keys.insert(owned);
    *bytes = next;
    *retained_bytes = total;
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

fn add_avg(sum: &mut f64, compensation: &mut f64, count: &mut i64, value: &Value) -> Result<()> {
    let number = match value {
        Value::Integer(number) => *number as f64,
        Value::Float(number) => *number,
        _ => return Err(Error::Type("AVG requires numeric values".into())),
    };
    // Match in-memory AVG: retain the bits lost when adding values
    // of very different magnitudes (Neumaier compensated summation).
    let next = *sum + number;
    let correction = if sum.abs() >= number.abs() {
        (*sum - next) + number
    } else {
        (number - next) + *sum
    };
    let next_compensation = *compensation + correction;
    if !next.is_finite() || !next_compensation.is_finite() ||
        !(next + next_compensation).is_finite() {
        return Err(Error::Execution("AVG overflow".into()));
    }
    let next_count = count.checked_add(1)
        .ok_or_else(|| Error::Execution("AVG count overflow".into()))?;
    *sum = next;
    *compensation = next_compensation;
    *count = next_count;
    Ok(())
}

fn add_to_state(
    state: &mut State, op: &AggregateOp, batch: &Batch, row: usize,
    direct_column: Option<usize>,
    retained_bytes: &mut usize, max_retained_bytes: usize,
) -> Result<()> {
    // A column reference already has its typed value in the decoded batch.
    // Only computed expressions need an owned scalar, which is important for
    // MIN/MAX on wide strings when most candidates never replace the extreme.
    let value: Cow<'_, Value> = match op {
        AggregateOp::CountAll => Cow::Owned(Value::Integer(1)),
        AggregateOp::Count(expr) | AggregateOp::CountDistinct(expr) | AggregateOp::Sum(expr) |
        AggregateOp::Avg(expr) | AggregateOp::Min(expr) |
        AggregateOp::Max(expr) | AggregateOp::SumDistinct(expr) |
        AggregateOp::AvgDistinct(expr) => match direct_column {
            Some(index) => Cow::Borrowed(batch.value(row, index)?),
            None => Cow::Owned(expr.evaluate(batch, row)?),
        },
    };
    if value.is_null() { return Ok(()); }
    let is_min = matches!(state, State::Min(_));
    match state {
        State::Count(count) => {
            *count = count.checked_add(1)
                .ok_or_else(|| Error::Execution("COUNT overflow".into()))?;
        }
        State::CountDistinct { keys, bytes } => {
            insert_distinct(keys, bytes, &value, retained_bytes, max_retained_bytes)?;
        }
        State::Sum(total) => add_sum(total, &value)?,
        State::SumDistinct { keys, bytes, total } => {
            if insert_distinct(keys, bytes, &value, retained_bytes, max_retained_bytes)? {
                add_sum(total, &value)?;
            }
        }
        State::Avg { sum, compensation, count } =>
            add_avg(sum, compensation, count, &value)?,
        State::AvgDistinct { keys, bytes, sum, compensation, count } => {
            if insert_distinct(keys, bytes, &value, retained_bytes, max_retained_bytes)? {
                add_avg(sum, compensation, count, &value)?;
            }
        }
        State::Min(current) | State::Max(current) => {
            if let Some(existing) = current {
                let order = value.sql_compare(existing)?
                    .ok_or_else(|| Error::Type("non-comparable MIN/MAX input".into()))?;
                if (is_min && order.is_lt()) || (!is_min && order.is_gt()) {
                    *existing = value.into_owned();
                }
            } else { *current = Some(value.into_owned()); }
        }
    }
    Ok(())
}

// Every partition uses the same schema. Resolve direct aggregate columns
// once rather than paying for a case-folding hash lookup for every row.
fn bind_aggregate_column(op: &AggregateOp, schema: &Schema) -> Result<Option<usize>> {
    let expression = match op {
        AggregateOp::CountAll => return Ok(None),
        AggregateOp::Count(expr) | AggregateOp::CountDistinct(expr) |
        AggregateOp::Sum(expr) | AggregateOp::SumDistinct(expr) |
        AggregateOp::Avg(expr) | AggregateOp::AvgDistinct(expr) |
        AggregateOp::Min(expr) | AggregateOp::Max(expr) => expr,
    };
    match expression {
        Expr::Column(name) => Ok(Some(schema.column(name)?)),
        _ => Ok(None),
    }
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

/// Compare a retained group key directly against borrowed batch columns.
/// Keep equality checks even after hashing to handle fingerprint collisions.
fn matches_group_row(key: &[Value], columns: &[&[Value]], row: usize) -> bool {
    key.len() == columns.len() &&
        key.iter().zip(columns).all(|(value, column)| value == &column[row])
}

fn aggregate_shard(
    path: &Path, input: &Schema, keys: &[usize],
    items: &[AggregateItem], output: &Schema,
    max_groups: usize, io_rows: usize, control: &QueryControl,
) -> Result<Batch> {
    aggregate_shard_with_budget(path, input, keys, items, output,
        max_groups, io_rows, MAX_GROUP_KEY_STATE_BYTES, control)
}

#[allow(clippy::too_many_arguments)]
fn aggregate_shard_with_budget(
    path: &Path, input: &Schema, keys: &[usize],
    items: &[AggregateItem], output: &Schema,
    max_groups: usize, io_rows: usize, max_key_state_bytes: usize,
    control: &QueryControl,
) -> Result<Batch> {
    let mut reader = SpillReader::new(BufReader::new(File::open(path)?), io_rows)?;
    if reader.schema() != input { return Err(Error::Schema("group shard schema mismatch".into())); }
    let direct_columns = items.iter().map(|item|
        bind_aggregate_column(&item.op, input)
    ).collect::<Result<Vec<_>>>()?;
    // Fingerprint borrowed keys on each row and materialize an owned key
    // only for the first occurrence of a group. Position vectors preserve
    // correctness under hash collisions without cloning duplicate TEXT keys.
    let mut groups: Vec<(Vec<Value>, Vec<State>)> = Vec::new();
    let mut by_hash: HashMap<u64, Vec<usize>> = HashMap::new();
    let mut retained_key_state_bytes = 0usize;
    while let Some(batch) = reader.next_batch()? {
        control.check()?;
        let key_columns = keys.iter().map(|&index| batch.column(index))
            .collect::<Result<Vec<_>>>()?;
        let mut previous_group: Option<usize> = None;
        for row in 0..batch.num_rows() {
            // Repeated adjacent groups are typical after hash-partitioned
            // sorting. Probe their retained key before hashing every cell.
            let position = if let Some(last) = previous_group.filter(|&position|
                matches_group_row(&groups[position].0, &key_columns, row)
            ) {
                last
            } else {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                key_columns.len().hash(&mut hasher);
                for column in &key_columns { column[row].hash(&mut hasher); }
                let fingerprint = hasher.finish();
                let existing = by_hash.get(&fingerprint).and_then(|positions| {
                    positions.iter().copied().find(|&position| {
                        matches_group_row(&groups[position].0, &key_columns, row)
                    })
                });
                if let Some(position) = existing {
                    position
                } else {
                    if groups.len() >= max_groups {
                        return Err(Error::Execution("group state count exceeds partition budget".into()));
                    }
                    // Charge only unique group keys before cloning any TEXT values.
                    let footprint = super::row_footprint::footprint(
                        key_columns.iter().map(|column| &column[row])
                    ).saturating_add(items.len().saturating_mul(std::mem::size_of::<State>()));
                    let next_bytes = retained_key_state_bytes.checked_add(footprint)
                        .filter(|total| *total <= max_key_state_bytes)
                        .ok_or_else(|| Error::Execution(
                            "group key state byte budget exceeded".into()))?;
                    let initial = items.iter().map(|item| new_state(&item.op))
                        .collect::<Result<Vec<_>>>()?;
                    let key = key_columns.iter().map(|column| column[row].clone())
                        .collect::<Vec<_>>();
                    let position = groups.len();
                    groups.push((key, initial));
                    by_hash.entry(fingerprint).or_default().push(position);
                    retained_key_state_bytes = next_bytes;
                    position
                }
            };
            previous_group = Some(position);
            for ((state, item), &direct_column) in groups[position].1.iter_mut()
                .zip(items).zip(&direct_columns) {
                add_to_state(state, &item.op, &batch, row, direct_column,
                    &mut retained_key_state_bytes, max_key_state_bytes)?;
            }
        }
    }
    let row_count = groups.len();
    let mut columns = (0..output.len())
        .map(|_| Vec::with_capacity(row_count)).collect::<Vec<_>>();
    for (mut key, states) in groups {
        for state in states {
            key.push(match state {
                State::Count(value) => Value::Integer(value),
                State::CountDistinct { keys, .. } => Value::Integer(keys.len() as i64),
                State::AvgDistinct { sum, compensation, count, .. } |
                State::Avg { sum, compensation, count } =>
                    if count == 0 {
                        Value::Null
                    } else {
                        Value::Float((sum + compensation) / count as f64)
                    },
                State::SumDistinct { total: v, .. } |
                State::Sum(v) | State::Min(v) | State::Max(v) => v.unwrap_or(Value::Null),
            });
        }
        // Keys and finalized aggregate values already belong to this shard.
        // Move them directly into the output columns, without allocating a
        // second row-major collection and transposing it again.
        for (column, value) in columns.iter_mut().zip(key) {
            column.push(value);
        }
    }
    Batch::new_with_row_count(output.clone(), columns, row_count)
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
        // A sparse hash exchange can have thousands of empty shards.
        // No grouped rows exist for them, so skip schema-only output files.
        if batch.num_rows() == 0 { continue; }
        let id = GROUP_ID.fetch_add(1, Ordering::Relaxed);
        let path = directory.join(format!("rustspark-groups-{}-{id}.rspill", std::process::id()));
        let file = super::private_files::create_private(&path)?;
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

#[cfg(test)]
mod grouped_spill_key_state_budget_tests {
    use super::*;
    use crate::schema::{DataType, Field};
    use std::{fs, io::BufWriter};

    #[test]
    fn unique_wide_keys_consume_budget_but_duplicates_do_not() {
        let dir = std::env::temp_dir().join(format!(
            "native-group-state-budget-{}-{}", std::process::id(),
            GROUP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        let path = dir.join("groups.rspill");
        let schema = Schema::new(vec![Field::new("k", DataType::Text, false)]).unwrap();
        let key = Value::Text("alpha".repeat(8));
        let other = Value::Text("beta".repeat(8));
        let source = Batch::from_rows(schema.clone(), vec![
            vec![key.clone()], vec![key.clone()], vec![other.clone()],
        ]).unwrap();
        let mut writer = SpillWriter::new(BufWriter::new(File::create(&path).unwrap()),
            schema.clone()).unwrap();
        writer.write_batch(&source).unwrap();
        writer.finish().unwrap();
        let items = [AggregateItem::new(AggregateOp::CountAll, "n")];
        let output = output_schema(&schema, &[("k".into(), "k".into())], &items).unwrap();
        let one_group_budget = super::super::row_footprint::footprint(
            std::iter::once(&key)).saturating_add(std::mem::size_of::<State>());
        let control = QueryControl::default();
        let err = aggregate_shard_with_budget(&path, &schema, &[0], &items, &output,
            10, 3, one_group_budget, &control).unwrap_err();
        assert!(err.to_string().contains("byte budget"));
        let actual = aggregate_shard_with_budget(&path, &schema, &[0], &items, &output,
            10, 3, 8192, &control).unwrap().rows().unwrap();
        assert_eq!(actual.len(), 2);
        assert!(actual.contains(&vec![key, Value::Integer(2)]));
        assert!(actual.contains(&vec![other, Value::Integer(1)]));
        assert!(path.exists());
        fs::remove_file(path).unwrap();
        fs::remove_dir(dir).unwrap();
    }
}

#[cfg(test)]
mod sparse_group_result_shards_tests {
    use super::*;
    use crate::{expr::Expr, schema::{DataType, Field}};
    use std::{fs, io::BufReader};

    #[test]
    fn writes_only_nonempty_group_results_for_sparse_partitions() {
        let dir = std::env::temp_dir().join(format!(
            "native-groups-sparse-{}-{}", std::process::id(),
            GROUP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("g", DataType::Integer, false),
            Field::new("value", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(5), Value::Integer(7)],
            vec![Value::Integer(5), Value::Integer(11)],
        ]).unwrap();
        let items = [AggregateItem::new(
            AggregateOp::Sum(Expr::Column("value".into())), "total")];
        let result = grouped_aggregate([Ok(batch)], schema.clone(),
            &[("g".into(), "g".into())], &items,
            &dir, 128, 10, 2, &QueryControl::default()).unwrap();
        assert_eq!(result.paths().len(), 1);
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
        let mut reader = SpillReader::new(BufReader::new(
            File::open(&result.paths()[0]).unwrap()), 2).unwrap();
        assert_eq!(reader.next_batch().unwrap().unwrap().rows().unwrap(),
            vec![vec![Value::Integer(5), Value::Integer(18)]]);
        assert!(reader.next_batch().unwrap().is_none());
        drop(result);
        let empty = grouped_aggregate(Vec::<Result<Batch>>::new(), schema,
            &[("g".into(), "g".into())], &items, &dir, 128, 10, 2,
            &QueryControl::default()).unwrap();
        assert!(empty.paths().is_empty());
        drop(empty);
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);
        fs::remove_dir(dir).unwrap();
    }
}

#[cfg(test)]
mod compensated_spill_average_tests {
    use super::*;
    use crate::{expr::Expr, schema::{DataType, Field}};
    use std::{fs, io::BufReader};

    #[test]
    fn cancellation_sensitive_averages_survive_partitioned_aggregation() {
        let dir = std::env::temp_dir().join(format!(
            "native-avg-compensated-{}-{}", std::process::id(),
            GROUP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![
            Field::new("group_id", DataType::Integer, false),
            Field::new("n", DataType::Float, false),
        ]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1), Value::Float(1e16)],
            vec![Value::Integer(1), Value::Float(1.0)],
            vec![Value::Integer(1), Value::Float(-1e16)],
            vec![Value::Integer(1), Value::Float(1.0)], // repeat DISTINCT value
        ]).unwrap();
        let items = [
            AggregateItem::new(AggregateOp::Avg(Expr::Column("n".into())), "avg"),
            AggregateItem::new(AggregateOp::AvgDistinct(Expr::Column("n".into())), "avg_unique"),
        ];
        let result = grouped_aggregate([Ok(batch)], schema,
            &[("group_id".into(), "group_id".into())], &items,
            &dir, 2, 10, 2, &QueryControl::default()).unwrap();
        let mut rows = Vec::new();
        for path in result.paths() {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 2).unwrap();
            while let Some(part) = reader.next_batch().unwrap() {
                rows.extend(part.rows().unwrap());
            }
        }
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::Float(0.5));
        assert_eq!(rows[0][2], Value::Float(1.0 / 3.0));
        drop(result);
        fs::remove_dir(dir).unwrap();
    }
}

#[cfg(test)]
mod prebound_group_keys_perf_tests {
    use super::*;
    use crate::schema::{DataType, Field};
    use std::{collections::HashMap, fs, sync::atomic::{AtomicU64, Ordering}};
    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn nullable_groups_across_many_spill_batches_keep_exact_counts() {
        let dir = std::env::temp_dir().join(format!("rustspark-prebound-groups-{}-{}",
            std::process::id(), TEST_ID.fetch_add(1, Ordering::Relaxed)));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("k", DataType::Integer, true)]).unwrap();
        let mut expected = HashMap::<Value, i64>::new();
        let rows = (0..2048_i64).map(|n| {
            let key = if n % 17 == 0 { Value::Null } else { Value::Integer(n % 13) };
            *expected.entry(key.clone()).or_default() += 1;
            vec![key]
        }).collect();
        let batch = Batch::from_rows(schema.clone(), rows).unwrap();
        let result = grouped_aggregate([Ok(batch)], schema,
            &[("k".into(), "k".into())],
            &[AggregateItem::new(AggregateOp::CountAll, "n")],
            &dir, 8, 64, 31, &QueryControl::default()).unwrap();
        let mut actual = HashMap::new();
        for path in result.paths() {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 31).unwrap();
            while let Some(batch) = reader.next_batch().unwrap() {
                for row in batch.rows().unwrap() {
                    let Value::Integer(count) = &row[1] else { panic!("invalid count") };
                    actual.insert(row[0].clone(), *count);
                }
            }
        }
        assert_eq!(actual, expected);
        drop(result);
        fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod grouped_spill_total_distinct_bytes_regressions {
    use super::*;
    use crate::schema::{DataType, Field};
    use std::{fs, io::BufWriter};

    #[test]
    fn distinct_values_share_partition_budget_and_repeats_cost_nothing() {
        let dir = std::env::temp_dir().join(format!(
            "native-distinct-partition-limit-{}-{}", std::process::id(),
            GROUP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        let path = dir.join("input.rspill");
        let schema = Schema::new(vec![
            Field::new("k", DataType::Integer, false),
            Field::new("v", DataType::Text, false),
        ]).unwrap();
        let first = Value::Text("first".repeat(10));
        let second = Value::Text("other".repeat(10));
        let mk = |value| vec![Value::Integer(1), value];
        let mut writer = SpillWriter::new(
            BufWriter::new(File::create(&path).unwrap()), schema.clone(),
        ).unwrap();
        writer.write_batch(&Batch::from_rows(schema.clone(), vec![
            mk(first.clone()), mk(first.clone()), mk(second.clone()),
        ]).unwrap()).unwrap();
        writer.finish().unwrap();
        let items = [AggregateItem::new(AggregateOp::CountDistinct(
            Expr::Column("v".into())), "n")];
        let output = output_schema(&schema, &[("k".into(), "k".into())], &items).unwrap();
        let fixed = super::super::row_footprint::footprint([&Value::Integer(1)].into_iter())
            + std::mem::size_of::<State>();
        let each = super::super::row_footprint::footprint([&first].into_iter());
        let control = QueryControl::default();
        let error = aggregate_shard_with_budget(&path, &schema, &[0], &items, &output,
            10, 3, fixed + each * 2 - 1, &control).unwrap_err();
        assert!(error.to_string().contains("partition byte budget"));
        let rows = aggregate_shard_with_budget(&path, &schema, &[0], &items, &output,
            10, 3, fixed + each * 2, &control).unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Integer(1), Value::Integer(2)]]);
        fs::remove_file(path).unwrap();
        fs::remove_dir(dir).unwrap();
    }

    #[test]
    fn multiple_groups_cannot_each_exhaust_the_full_partition_budget() {
        let mut keys_a = HashSet::new();
        let mut bytes_a = 0;
        let mut keys_b = HashSet::new();
        let mut bytes_b = 0;
        let value = Value::Text("shared".repeat(20));
        let budget = super::super::row_footprint::footprint([&value].into_iter());
        let mut retained = 0;
        assert!(insert_distinct(&mut keys_a, &mut bytes_a, &value, &mut retained, budget).unwrap());
        assert!(!insert_distinct(&mut keys_a, &mut bytes_a, &value, &mut retained, budget).unwrap());
        assert!(insert_distinct(&mut keys_b, &mut bytes_b, &value, &mut retained, budget).is_err());
        assert_eq!(keys_a.len(), 1);
        assert!(keys_b.is_empty());
        assert_eq!(retained, budget);
    }
}

#[cfg(test)]
mod columnar_grouped_shard_output_tests {
    use super::*;
    use crate::{expr::Expr, scalar::Value, schema::{DataType, Field}};
    use std::{fs, io::BufWriter};

    #[test]
    fn materializes_nullable_unicode_group_keys_and_multiple_aggregate_states() {
        let path = std::env::temp_dir().join(format!(
            "native-groups-columnar-{}-{}.rspill", std::process::id(),
            GROUP_ID.fetch_add(1, Ordering::Relaxed),
        ));
        let input = Schema::new(vec![
            Field::new("tag", DataType::Text, true),
            Field::new("amount", DataType::Integer, true),
        ]).unwrap();
        let batch = Batch::from_rows(input.clone(), vec![
            vec![Value::Text("東京".into()), Value::Integer(5)],
            vec![Value::Null, Value::Integer(2)],
            vec![Value::Text("東京".into()), Value::Integer(7)],
            vec![Value::Text("東京".into()), Value::Null],
        ]).unwrap();
        let items = vec![
            AggregateItem { alias: "rows".into(), op: AggregateOp::CountAll },
            AggregateItem { alias: "total".into(), op: AggregateOp::Sum(
                Expr::Column("amount".into())) },
        ];
        let output = output_schema(&input, &[("tag".into(), "tag".into())], &items).unwrap();
        let mut writer = SpillWriter::new(
            BufWriter::new(File::create(&path).unwrap()), input.clone(),
        ).unwrap();
        writer.write_batch(&batch).unwrap();
        writer.finish().unwrap();
        let actual = aggregate_shard(&path, &input, &[0], &items, &output, 10, 2,
            &QueryControl::default()).unwrap();
        let mut rows = actual.rows().unwrap();
        rows.sort_by_key(|row| match &row[0] {
            Value::Null => 0, Value::Text(_) => 1, _ => 2,
        });
        assert_eq!(rows, vec![
            vec![Value::Null, Value::Integer(1), Value::Integer(2)],
            vec![Value::Text("東京".into()), Value::Integer(3), Value::Integer(12)],
        ]);
        fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod borrowed_group_key_equality_regressions {
    use super::*;

    #[test]
    fn compares_nullable_unicode_and_canonical_float_values_without_cloning() {
        let key = vec![
            Value::Text("東京🙂".repeat(1000)),
            Value::Float(f64::NAN), Value::Null,
        ];
        let columns = vec![
            vec![Value::Text("東京🙂".repeat(1000)), Value::Text("different".into())],
            vec![Value::Float(-f64::NAN), Value::Float(0.0)],
            vec![Value::Null, Value::Null],
        ];
        let borrowed = columns.iter().map(Vec::as_slice).collect::<Vec<_>>();
        assert!(matches_group_row(&key, &borrowed, 0));
        assert!(!matches_group_row(&key, &borrowed, 1));
        assert!(!matches_group_row(&key[..2], &borrowed, 0));
    }

    #[test]
    fn collision_candidate_lookup_requires_exact_group_equality() {
        let keys = vec![
            vec![Value::Text("group-1".into()), Value::Integer(1)],
            vec![Value::Text("group-2".into()), Value::Integer(1)],
        ];
        let columns = vec![
            vec![Value::Text("group-2".into())],
            vec![Value::Integer(1)],
        ];
        let borrowed = columns.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let candidate_bucket = [0usize, 1usize];
        let matched = candidate_bucket.into_iter().find(|&position| {
            matches_group_row(&keys[position], &borrowed, 0)
        });
        assert_eq!(matched, Some(1));
    }
}

#[cfg(test)]
mod borrowed_group_aggregate_regressions {
    use super::*;

    #[test]
    fn wide_text_min_max_and_distinct_match_owned_evaluation() {
        let schema = Schema::new(vec![
            Field::new("payload", DataType::Text, true),
        ]).unwrap();
        let big = "🙂".repeat(100_000);
        let batches = Batch::from_rows(schema, vec![
            vec![Value::Text(format!("b{big}"))],
            vec![Value::Text(format!("z{big}"))],
            vec![Value::Text(format!("a{big}"))],
            vec![Value::Text(format!("z{big}"))],
            vec![Value::Null],
        ]).unwrap();
        let expression = Expr::Column("payload".into());
        let mut minimum = new_state(&AggregateOp::Min(expression.clone())).unwrap();
        let mut maximum = new_state(&AggregateOp::Max(expression.clone())).unwrap();
        let mut distinct = new_state(&AggregateOp::CountDistinct(expression.clone())).unwrap();
        let mut bytes = 0usize;
        for row in 0..batches.num_rows() {
            add_to_state(&mut minimum, &AggregateOp::Min(expression.clone()), &batches, row,
                Some(0), &mut bytes, 32 * 1024 * 1024).unwrap();
            add_to_state(&mut maximum, &AggregateOp::Max(expression.clone()), &batches, row,
                Some(0), &mut bytes, 32 * 1024 * 1024).unwrap();
            add_to_state(&mut distinct, &AggregateOp::CountDistinct(expression.clone()), &batches, row,
                Some(0), &mut bytes, 32 * 1024 * 1024).unwrap();
        }
        assert!(matches!(minimum, State::Min(Some(Value::Text(ref text))) if text.starts_with('a')));
        assert!(matches!(maximum, State::Max(Some(Value::Text(ref text))) if text.starts_with('z')));
        assert!(matches!(distinct, State::CountDistinct { ref keys, .. } if keys.len() == 3));
    }

    #[test]
    fn invalid_column_still_reports_an_error() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(1)]],
        ).unwrap();
        let op = AggregateOp::Min(Expr::Column("missing".into()));
        let mut state = new_state(&op).unwrap();
        let mut bytes = 0;
        assert!(add_to_state(&mut state, &op, &batch, 0, None, &mut bytes, 1024).is_err());
    }
}

#[cfg(test)]
mod bound_group_aggregate_columns_regressions {
    use super::*;
    use crate::{expr::BinaryOp, schema::{DataType, Field}};
    use std::{fs, io::BufReader};

    #[test]
    fn direct_and_computed_aggregates_match_over_repeated_spill_batches() {
        let directory = std::env::temp_dir().join(format!(
            "rustspark-bound-aggregate-{}-{}", std::process::id(),
            GROUP_ID.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&directory).unwrap();
        let schema = Schema::new(vec![
            Field::new("GroupID", DataType::Integer, false),
            Field::new("Number", DataType::Integer, true),
        ]).unwrap();
        let rows = (0..64).flat_map(|_| [
            vec![Value::Integer(1), Value::Integer(2)],
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Integer(5)],
        ]).collect();
        let batch = Batch::from_rows(schema.clone(), rows).unwrap();
        let n = Expr::Column("NUMBER".into());
        let items = [
            AggregateItem::new(AggregateOp::Sum(n.clone()), "sum_n"),
            AggregateItem::new(AggregateOp::CountDistinct(n.clone()), "distinct_n"),
            AggregateItem::new(AggregateOp::Sum(Expr::binary(
                n, BinaryOp::Add, Expr::Literal(Value::Integer(1)),
            )), "sum_plus_one"),
        ];
        let result = grouped_aggregate(
            [Ok(batch)], schema, &[("GROUPID".into(), "g".into())],
            &items, &directory, 4, 4, 3, &QueryControl::default(),
        ).unwrap();
        let mut rows = Vec::new();
        for path in result.paths() {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 3).unwrap();
            while let Some(batch) = reader.next_batch().unwrap() { rows.extend(batch.rows().unwrap()); }
        }
        rows.sort_by(|a, b| a[0].sql_compare(&b[0]).unwrap().unwrap());
        assert_eq!(rows, vec![
            vec![Value::Integer(1), Value::Integer(128), Value::Integer(1), Value::Integer(192)],
            vec![Value::Integer(2), Value::Integer(320), Value::Integer(1), Value::Integer(384)],
        ]);
        drop(result);
        fs::remove_dir(directory).unwrap();
    }
}

#[cfg(test)]
mod consecutive_group_spill_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64 as TestAtomic, Ordering as TestOrdering};

    static NEXT_DIR: TestAtomic = TestAtomic::new(1);

    #[test]
    fn sorted_unicode_groups_keep_sum_and_distinct_across_spill_windows() {
        let directory = std::env::temp_dir().join(format!(
            "rustspark-group-runs-{}-{}", std::process::id(),
            NEXT_DIR.fetch_add(1, TestOrdering::Relaxed),
        ));
        std::fs::create_dir(&directory).unwrap();
        let schema = Schema::new(vec![
            Field::new("key", DataType::Text, true),
            Field::new("amount", DataType::Integer, true),
        ]).unwrap();
        let a = Value::Text("🦀".repeat(512));
        let b = Value::Text("x".repeat(1000));
        let mut rows = Vec::new();
        for _ in 0..300 { rows.push(vec![a.clone(), Value::Integer(2)]); }
        for _ in 0..200 { rows.push(vec![Value::Null, Value::Null]); }
        for _ in 0..200 { rows.push(vec![b.clone(), Value::Integer(3)]); }
        for _ in 0..150 { rows.push(vec![a.clone(), Value::Integer(4)]); }
        let batch = Batch::from_rows(schema.clone(), rows).unwrap();
        let items = [
            AggregateItem::new(AggregateOp::Sum(Expr::Column("amount".into())), "total"),
            AggregateItem::new(AggregateOp::CountDistinct(Expr::Column("amount".into())), "unique"),
        ];
        let files = grouped_aggregate(
            [Ok(batch)], schema, &[("key".into(), "key".into())],
            &items, &directory, 4, 10, 1024, &QueryControl::default(),
        ).unwrap();
        let mut results = std::collections::HashMap::new();
        for path in files.paths() {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 1024).unwrap();
            while let Some(batch) = reader.next_batch().unwrap() {
                for mut row in batch.rows().unwrap() {
                    results.insert(row.remove(0), row);
                }
            }
        }
        assert_eq!(results.get(&a), Some(&vec![Value::Integer(1200), Value::Integer(2)]));
        assert_eq!(results.get(&b), Some(&vec![Value::Integer(600), Value::Integer(1)]));
        assert_eq!(results.get(&Value::Null), Some(&vec![Value::Null, Value::Integer(0)]));
        drop(files);
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[cfg(test)]
mod cloned_distinct_budget_regressions {
    use super::*;

    #[test]
    fn oversized_input_capacity_does_not_consume_the_owned_key_budget() {
        let mut overallocated = String::with_capacity(32 * 1024);
        overallocated.push_str("a small key");
        let key = Value::Text(overallocated);
        let mut keys = HashSet::new();
        let mut group_bytes = 0;
        let mut shard_bytes = 0;
        assert!(insert_distinct(
            &mut keys, &mut group_bytes, &key, &mut shard_bytes, 256,
        ).unwrap());
        assert_eq!(group_bytes, shard_bytes);
        assert!(group_bytes <= 256);
        assert_eq!(keys.len(), 1);
        assert!(!insert_distinct(
            &mut keys, &mut group_bytes, &key, &mut shard_bytes, 256,
        ).unwrap());
        assert_eq!(group_bytes, shard_bytes);
    }

    #[test]
    fn real_text_overflow_fails_without_inserting_or_charging() {
        let large = Value::Text("x".repeat(4096));
        let mut keys = HashSet::new();
        let mut bytes = 0;
        let mut retained = 0;
        assert!(insert_distinct(
            &mut keys, &mut bytes, &large, &mut retained, 256,
        ).is_err());
        assert_eq!(bytes, 0);
        assert_eq!(retained, 0);
        assert!(keys.is_empty());
        let small = Value::Text("okay".into());
        assert!(insert_distinct(
            &mut keys, &mut bytes, &small, &mut retained, 256,
        ).unwrap());
        assert!(retained > 0);
    }
}

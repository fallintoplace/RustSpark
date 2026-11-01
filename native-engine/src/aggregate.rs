use std::{borrow::Cow, collections::{HashMap, HashSet}, hash::{Hash, Hasher}};

use crate::{
    batch::Batch,
    expr::Expr,
    scalar::Value,
    schema::{DataType, Field, Schema},
    Error,
    Result,
};

#[derive(Clone, Debug, PartialEq)]
pub enum AggregateOp {
    CountAll,
    Count(Expr),
    CountDistinct(Expr),
    Sum(Expr),
    SumDistinct(Expr),
    Avg(Expr),
    AvgDistinct(Expr),
    Min(Expr),
    Max(Expr),
    BitXor(Expr),
    BitOr(Expr),
    BitAnd(Expr),
}

// The engine validates query plans, but public aggregation helpers may be
// called directly. Check expression complexity before recursive type inference
// or row-wise evaluation, including for empty inputs.
fn validate_aggregate_items(items: &[AggregateItem]) -> Result<()> {
    for item in items {
        match &item.op {
            AggregateOp::CountAll => {},
            AggregateOp::Count(expr) | AggregateOp::CountDistinct(expr)
            | AggregateOp::Sum(expr) | AggregateOp::SumDistinct(expr)
            | AggregateOp::Avg(expr) | AggregateOp::AvgDistinct(expr)
            | AggregateOp::Min(expr) | AggregateOp::Max(expr) 
            | AggregateOp::BitAnd(expr) 
            | AggregateOp::BitOr(expr) 
            | AggregateOp::BitXor(expr) => expr.validate_budget()?,
        }
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregateItem {
    pub op: AggregateOp,
    pub alias: String,
}

impl AggregateItem {
    pub fn new(op: AggregateOp, alias: impl Into<String>) -> Self {
        Self {
            op,
            alias: alias.into(),
        }
    }

    fn output_type(&self, schema: &Schema) -> Result<DataType> {
        match &self.op {
            AggregateOp::CountAll => Ok(DataType::Integer),
            AggregateOp::Count(expr) | AggregateOp::CountDistinct(expr) => {
                expr.output_type(schema)?;
                Ok(DataType::Integer)
            },
            AggregateOp::Sum(expr) | AggregateOp::SumDistinct(expr) => {
                let kind = expr.output_type(schema)?;
                match kind {
                    DataType::Integer | DataType::Float => Ok(kind),
                    _ => Err(Error::Type("SUM requires a numeric column".into())),
                }
            }
            AggregateOp::Avg(expr) | AggregateOp::AvgDistinct(expr) => {
                match expr.output_type(schema)? {
                    DataType::Integer | DataType::Float => Ok(DataType::Float),
                    _ => Err(Error::Type("AVG requires a numeric column".into())),
                }
            }
            AggregateOp::Min(expr) | AggregateOp::Max(expr) => expr.output_type(schema),
            AggregateOp::BitAnd(expr) => {
                let kind = if matches!(expr, Expr::Literal(Value::Null)) {
                    DataType::Integer
                } else {
                    expr.output_type(schema)?
                };
                if kind == DataType::Integer { Ok(DataType::Integer) }
                else { Err(Error::Type("BIT_AND requires integer input".into())) }
            },
            AggregateOp::BitOr(expr) => {
                let kind = if matches!(expr, Expr::Literal(Value::Null)) {
                    DataType::Integer
                } else {
                    expr.output_type(schema)?
                };
                if kind == DataType::Integer { Ok(DataType::Integer) }
                else { Err(Error::Type("BIT_OR requires integer input".into())) }
            },
            AggregateOp::BitXor(expr) => {
                let kind = if matches!(expr, Expr::Literal(Value::Null)) {
                    DataType::Integer
                } else {
                    expr.output_type(schema)?
                };
                if kind == DataType::Integer { Ok(DataType::Integer) }
                else { Err(Error::Type("BIT_XOR requires integer input".into())) }
            },
        }
    }
}

#[derive(Clone)]
enum State {
    Count(i64),
    CountDistinct { seen: HashSet<Value>, bytes: usize },
    Sum(Option<Value>),
    SumDistinct { seen: HashSet<Value>, total: Option<Value> },
    Avg { sum: f64, compensation: f64, count: i64 },
    AvgDistinct { seen: HashSet<Value>, sum: f64, compensation: f64, count: i64 },
    Min(Option<Value>),
    Max(Option<Value>),
    BitXor(Option<i64>),
    BitOr(Option<i64>),
    BitAnd(Option<i64>),
}

impl State {
    fn new(op: &AggregateOp) -> Self {
        match op {
            AggregateOp::CountAll | AggregateOp::Count(_) => Self::Count(0),
            AggregateOp::CountDistinct(_) => Self::CountDistinct { seen: HashSet::new(), bytes: 0 },
            AggregateOp::Sum(_) => Self::Sum(None),
            AggregateOp::SumDistinct(_) => Self::SumDistinct { seen: HashSet::new(), total: None },
            AggregateOp::Avg(_) => Self::Avg { sum: 0.0, compensation: 0.0, count: 0 },
            AggregateOp::AvgDistinct(_) =>
                Self::AvgDistinct { seen: HashSet::new(), sum: 0.0, compensation: 0.0, count: 0 },
            AggregateOp::Min(_) => Self::Min(None),
            AggregateOp::Max(_) => Self::Max(None),
            AggregateOp::BitXor(_) => Self::BitXor(None),
            AggregateOp::BitOr(_) => Self::BitOr(None),
            AggregateOp::BitAnd(_) => Self::BitAnd(None),
        }
    }

    fn update(
        &mut self, op: &AggregateOp, batch: &Batch, row: usize,
        total_distinct_bytes: &mut usize, available_distinct_bytes: usize,
    ) -> Result<()> {
        // Direct column aggregates can borrow the typed cell. Only computed
        // expressions must own a temporary. COUNT and duplicate DISTINCT
        // rows should never clone large TEXT values just to inspect them.
        let value: Cow<'_, Value> = match op {
            AggregateOp::CountAll => Cow::Owned(Value::Integer(1)),
            AggregateOp::Count(expr)
            | AggregateOp::CountDistinct(expr)
            | AggregateOp::Sum(expr)
            | AggregateOp::SumDistinct(expr)
            | AggregateOp::Avg(expr)
            | AggregateOp::AvgDistinct(expr)
            | AggregateOp::Min(expr)
            | AggregateOp::Max(expr) 
            | AggregateOp::BitAnd(expr) 
            | AggregateOp::BitOr(expr) 
            | AggregateOp::BitXor(expr) => match expr {
                Expr::Column(name) =>
                    Cow::Borrowed(batch.value(row, batch.schema().column(name)?)?),
                _ => Cow::Owned(expr.evaluate(batch, row)?),
            },
        };
        match self {
            Self::Count(count) => {
                if !value.is_null() {
                    *count = count
                        .checked_add(1)
                        .ok_or_else(|| Error::Execution("COUNT overflow".into()))?;
                }
            }
            Self::CountDistinct { seen, bytes } => {
                if !value.is_null() && !seen.contains(value.as_ref()) {
                    if seen.len() >= MAX_DISTINCT_NUMERIC_VALUES_PER_GROUP {
                        return Err(Error::Execution("COUNT DISTINCT key budget exceeded".into()));
                    }
                    let footprint = distinct_key_footprint(value.as_ref())?;
                    let total = bytes.checked_add(footprint)
                        .filter(|size| *size <= 64 * 1024 * 1024)
                        .ok_or_else(|| Error::Execution(
                            "COUNT DISTINCT byte budget exceeded".into()))?;
                    let next_total = charge_distinct_size(
                        *total_distinct_bytes, footprint, available_distinct_bytes)?;
                    seen.insert(value.into_owned());
                    *bytes = total;
                    *total_distinct_bytes = next_total;
                }
            }
            Self::Sum(total) => add_sum(total, value.as_ref())?,
            Self::SumDistinct { seen, total } => {
                if !value.is_null() && !seen.contains(value.as_ref()) {
                    check_distinct_budget(seen)?;
                    let next_total = charge_distinct_size(
                        *total_distinct_bytes, distinct_key_footprint(value.as_ref())?,
                        available_distinct_bytes)?;
                    add_sum(total, value.as_ref())?;
                    seen.insert(value.into_owned());
                    *total_distinct_bytes = next_total;
                }
            }
            Self::Avg { sum, compensation, count } =>
                add_avg(sum, compensation, count, value.as_ref())?,
            Self::AvgDistinct { seen, sum, compensation, count } => {
                if !value.is_null() && !seen.contains(value.as_ref()) {
                    check_distinct_budget(seen)?;
                    let next_total = charge_distinct_size(
                        *total_distinct_bytes, distinct_key_footprint(value.as_ref())?,
                        available_distinct_bytes)?;
                    add_avg(sum, compensation, count, value.as_ref())?;
                    seen.insert(value.into_owned());
                    *total_distinct_bytes = next_total;
                }
            }
            Self::Min(minimum) => update_extreme(minimum, value, true)?,
            Self::Max(maximum) => update_extreme(maximum, value, false)?,
            Self::BitAnd(aggregate) => match value.as_ref() {
                Value::Null => {},
                Value::Integer(number) => {
                    *aggregate = Some((*aggregate).map_or(*number, |prior| prior & *number));
                },
                _ => return Err(Error::Type("BIT_AND requires integer input".into())),
            },
            Self::BitOr(aggregate) => match value.as_ref() {
                Value::Null => {},
                Value::Integer(number) => {
                    *aggregate = Some((*aggregate).map_or(*number, |prior| prior | *number));
                },
                _ => return Err(Error::Type("BIT_OR requires integer input".into())),
            },
            Self::BitXor(aggregate) => match value.as_ref() {
                Value::Null => {},
                Value::Integer(number) => {
                    *aggregate = Some((*aggregate).map_or(*number, |prior| prior ^ *number));
                },
                _ => return Err(Error::Type("BIT_XOR requires integer input".into())),
            },
        }
        Ok(())
    }

    fn finish(self) -> Result<Value> {
        Ok(match self {
            Self::Count(value) => Value::Integer(value),
            Self::CountDistinct { seen, .. } => Value::Integer(
                i64::try_from(seen.len())
                    .map_err(|_| Error::Execution("COUNT DISTINCT overflow".into()))?
            ),
            Self::Avg { sum, compensation, count } =>
                if count == 0 { Value::Null } else { Value::Float((sum + compensation) / count as f64) },
            Self::AvgDistinct { sum, compensation, count, .. } =>
                if count == 0 { Value::Null } else { Value::Float((sum + compensation) / count as f64) },
            Self::SumDistinct { total, .. } => total.unwrap_or(Value::Null),
            Self::BitAnd(value) => value.map(Value::Integer).unwrap_or(Value::Null),
            Self::BitOr(value) => value.map(Value::Integer).unwrap_or(Value::Null),
            Self::BitXor(value) => value.map(Value::Integer).unwrap_or(Value::Null),
            Self::Sum(value) | Self::Min(value) | Self::Max(value) => {
                value.unwrap_or(Value::Null)
            }
        })
    }
}

#[cfg(test)]
mod borrowed_aggregate_cells_tests {
    use super::*;
    use crate::{expr::BinaryOp, schema::{DataType, Field, Schema}};

    #[test]
    fn text_aggregates_and_computed_numeric_expressions_agree() {
        let huge = "x".repeat(32 * 1024);
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("label", DataType::Text, true),
            Field::new("n", DataType::Integer, false),
        ]).unwrap(), vec![
            vec![Value::Text(format!("z{huge}")), Value::Integer(2)],
            vec![Value::Text(format!("z{huge}")), Value::Integer(3)],
            vec![Value::Null, Value::Integer(1)],
            vec![Value::Text(format!("a{huge}")), Value::Integer(4)],
        ]).unwrap();
        let items = vec![
            AggregateItem { op: AggregateOp::Count(Expr::Column("label".into())), alias: "count".into() },
            AggregateItem { op: AggregateOp::CountDistinct(Expr::Column("label".into())), alias: "unique".into() },
            AggregateItem { op: AggregateOp::Min(Expr::Column("label".into())), alias: "min".into() },
            AggregateItem { op: AggregateOp::Max(Expr::Column("label".into())), alias: "max".into() },
            AggregateItem { op: AggregateOp::Sum(Expr::binary(
                Expr::Column("n".into()), BinaryOp::Add,
                Expr::Literal(Value::Integer(1)))), alias: "total".into() },
        ];
        assert_eq!(aggregate(&batch, &[], &items).unwrap().row(0).unwrap(), vec![
            Value::Integer(3), Value::Integer(2),
            Value::Text(format!("a{huge}")), Value::Text(format!("z{huge}")),
            Value::Integer(14),
        ]);
    }
}

const MAX_DISTINCT_NUMERIC_VALUES_PER_GROUP: usize = 65_536;

// Every DISTINCT key retains a HashSet entry and a cloned scalar. Charge
// all groups and all DISTINCT aggregates against one query-scoped budget.
fn distinct_key_footprint(value: &Value) -> Result<usize> {
    let payload = match value {
        Value::Text(text) => text.len(),
        _ => 0,
    };
    64usize.checked_add(std::mem::size_of::<Value>())
        .and_then(|fixed| fixed.checked_add(payload))
        .ok_or_else(|| Error::Execution("DISTINCT key footprint overflow".into()))
}

fn charge_distinct_size(current: usize, footprint: usize, limit: usize) -> Result<usize> {
    current.checked_add(footprint).filter(|&size| size <= limit)
        .ok_or_else(|| Error::Execution("aggregate DISTINCT total byte budget exceeded".into()))
}

fn check_distinct_budget(values: &HashSet<Value>) -> Result<()> {
    if values.len() >= MAX_DISTINCT_NUMERIC_VALUES_PER_GROUP {
        Err(Error::Execution("DISTINCT numeric aggregate key budget exceeded".into()))
    } else {
        Ok(())
    }
}

fn add_sum(total: &mut Option<Value>, value: &Value) -> Result<()> {
    if value.is_null() { return Ok(()); }
    let updated = match (total.as_ref(), value) {
        (None, Value::Integer(_) | Value::Float(_)) => value.clone(),
        (Some(Value::Integer(a)), Value::Integer(b)) => Value::Integer(
            a.checked_add(*b).ok_or_else(|| Error::Execution("SUM overflow".into()))?,
        ),
        (Some(Value::Integer(a)), Value::Float(b)) => Value::Float(*a as f64 + *b),
        (Some(Value::Float(a)), Value::Integer(b)) => Value::Float(*a + *b as f64),
        (Some(Value::Float(a)), Value::Float(b)) => Value::Float(*a + *b),
        _ => return Err(Error::Type("SUM requires numeric values".into())),
    };
    if matches!(&updated, Value::Float(number) if !number.is_finite()) {
        return Err(Error::Execution("SUM overflow".into()));
    }
    *total = Some(updated);
    Ok(())
}

fn add_avg(sum: &mut f64, compensation: &mut f64, count: &mut i64, value: &Value) -> Result<()> {
    let number = match value {
        Value::Null => return Ok(()),
        Value::Integer(value) => *value as f64,
        Value::Float(value) => *value,
        _ => return Err(Error::Type("AVG requires numeric values".into())),
    };
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

fn update_extreme(current: &mut Option<Value>, value: Cow<'_, Value>, minimum: bool) -> Result<()> {
    if value.is_null() {
        return Ok(());
    }
    if let Some(existing) = current {
        let comparison = value
            .sql_compare(existing)?
            .ok_or_else(|| Error::Type("MIN/MAX cannot compare NaN".into()))?;
        if (minimum && comparison.is_lt()) || (!minimum && comparison.is_gt()) {
            *existing = value.into_owned();
        }
    } else {
        *current = Some(value.into_owned());
    }
    Ok(())
}

pub fn aggregate(
    batch: &Batch,
    group_by: &[(Expr, String)],
    items: &[AggregateItem],
) -> Result<Batch> {
    aggregate_with_max_groups(batch, group_by, items, 1_000_000)
}

/// Derive the single-row result schema without scanning an input source.
pub fn global_output_schema(input: &Schema, items: &[AggregateItem]) -> Result<Schema> {
    validate_aggregate_items(items)?;
    let mut fields = Vec::with_capacity(items.len());
    for item in items {
        if let AggregateOp::Count(expr) | AggregateOp::CountDistinct(expr) = &item.op {
            expr.output_type(input)?;
        }
        let nullable = !matches!(&item.op,
            AggregateOp::CountAll | AggregateOp::Count(_) | AggregateOp::CountDistinct(_));
        fields.push(Field::new(&item.alias, item.output_type(input)?, nullable));
    }
    Schema::new(fields)
}

/// Update native scalar aggregate states incrementally across input batches.
/// DISTINCT aggregates need a separate bounded spill path and are rejected here.
pub fn aggregate_global_stream(
    source: impl Iterator<Item = Result<Batch>>,
    input_schema: &Schema, items: &[AggregateItem],
    mut check: impl FnMut() -> Result<()>,
) -> Result<Batch> {
    if items.iter().any(|item| matches!(&item.op,
        AggregateOp::CountDistinct(_) | AggregateOp::SumDistinct(_) |
        AggregateOp::AvgDistinct(_))) {
        return Err(Error::Unsupported("stream global DISTINCT aggregates require a spill path".into()));
    }
    let schema = global_output_schema(input_schema, items)?;
    let mut states = items.iter().map(|item| State::new(&item.op))
        .collect::<Vec<_>>();
    // The streaming path rejects DISTINCT during preflight above, so it
    // never charges this counter. Keep the state transition shared.
    let mut distinct_bytes = 0usize;
    for next in source {
        check()?;
        let batch = next?;
        if batch.schema() != input_schema {
            return Err(Error::Schema("stream aggregate input schema changed".into()));
        }
        for row in 0..batch.num_rows() {
            if row % 1024 == 0 { check()?; }
            for (state, item) in states.iter_mut().zip(items) {
                state.update(&item.op, &batch, row, &mut distinct_bytes,
                    MAX_GROUP_KEY_BYTES)?;
            }
        }
    }
    check()?;
    let row = states.into_iter().map(State::finish).collect::<Result<Vec<_>>>()?;
    Batch::from_rows(schema, vec![row])
}

const MAX_GROUP_KEY_BYTES: usize = 64 * 1024 * 1024;

fn group_key_state_bytes(key: &[Value], state_count: usize) -> Result<usize> {
    // Conservatively account for hash buckets, retained keys and per-group
    // aggregate states before admitting a new group.
    let fixed = 128usize.checked_add(state_count.checked_mul(std::mem::size_of::<State>())
        .ok_or_else(|| Error::Execution("GROUP BY key size overflow".into()))?)
        .ok_or_else(|| Error::Execution("GROUP BY key size overflow".into()))?;
    key.iter().try_fold(fixed, |used, value| {
        let text_bytes = match value {
            Value::Text(text) => text.len().checked_mul(2)
                .ok_or_else(|| Error::Execution("GROUP BY key size overflow".into()))?,
            _ => 0,
        };
        let slot = std::mem::size_of::<Value>().checked_mul(2)
            .and_then(|n| n.checked_add(text_bytes))
            .ok_or_else(|| Error::Execution("GROUP BY key size overflow".into()))?;
        used.checked_add(slot)
            .ok_or_else(|| Error::Execution("GROUP BY key size overflow".into()))
    })
}

pub fn aggregate_with_max_groups(
    batch: &Batch,
    group_by: &[(Expr, String)],
    items: &[AggregateItem],
    max_groups: usize,
) -> Result<Batch> {
    aggregate_with_limits(batch, group_by, items, max_groups, MAX_GROUP_KEY_BYTES)
}

/// Bound the memory retained by GROUP BY keys as well as group cardinality.
pub fn aggregate_with_limits(
    batch: &Batch,
    group_by: &[(Expr, String)],
    items: &[AggregateItem],
    max_groups: usize,
    max_key_state_bytes: usize,
) -> Result<Batch> {
    for (expr, _) in group_by { expr.validate_budget()?; }
    validate_aggregate_items(items)?;
    let mut fields = Vec::with_capacity(group_by.len() + items.len());
    for (expr, alias) in group_by {
        fields.push(Field::new(alias, expr.output_type(batch.schema())?, true));
    }
    for item in items {
        let nullable = !matches!(&item.op,
            AggregateOp::CountAll | AggregateOp::Count(_) | AggregateOp::CountDistinct(_));
        fields.push(Field::new(&item.alias, item.output_type(batch.schema())?, nullable));
    }
    let schema = Schema::new(fields)?;
    // Fingerprints point to candidate groups. Full key equality resolves
    // hash collisions, while repeated TEXT keys stay borrowed from input.
    let mut group_indices: HashMap<u64, Vec<usize>> = HashMap::new();
    let mut groups: Vec<(Vec<Value>, Vec<State>)> = Vec::new();
    let mut retained_group_bytes = 0usize;
    let mut retained_distinct_bytes = 0usize;

    if group_by.is_empty() {
        if max_groups == 0 {
            return Err(Error::Execution("GROUP BY group count exceeds memory budget".into()));
        }
        let size = group_key_state_bytes(&[], items.len())?;
        if size > max_key_state_bytes {
            return Err(Error::Execution("GROUP BY key state byte budget exceeded".into()));
        }
        retained_group_bytes = size;
        groups.push((Vec::new(), items.iter().map(|item| State::new(&item.op)).collect()));
    }
    let bound_columns = group_by.iter().map(|(expr, _)| match expr {
        Expr::Column(name) => Ok(Some(batch.column(batch.schema().column(name)?)?)),
        _ => Ok(None),
    }).collect::<Result<Vec<_>>>()?;
    // Reuse the key vector for repeated groups rather than allocating one
    // temporary vector per row in the common GROUP BY hot path.
    let mut key = Vec::with_capacity(group_by.len());
    let direct_group_keys = bound_columns.iter().all(Option::is_some);
    let mut previous_group: Option<usize> = None;
    for row in 0..batch.num_rows() {
        let index = if group_by.is_empty() {
            0
        } else if direct_group_keys && row > 0 && bound_columns.iter().all(|column| {
            let values = column.expect("all grouping keys are direct columns");
            values[row] == values[row - 1]
        }) {
            // The same typed composite key as the previous row maps to the
            // same group. Avoid hashing long clustered keys and probing the
            // fingerprint index. Computed keys still evaluate on every row.
            previous_group.expect("the previous row resolved a group")
        } else {
            key.clear();
            for ((expr, _), column) in group_by.iter().zip(&bound_columns) {
                key.push(match column {
                    Some(values) => std::borrow::Cow::Borrowed(&values[row]),
                    None => std::borrow::Cow::Owned(expr.evaluate(batch, row)?),
                });
            }
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            key.len().hash(&mut hasher);
            for value in &key { value.as_ref().hash(&mut hasher); }
            let fingerprint = hasher.finish();
            let existing = group_indices.get(&fingerprint)
                .and_then(|candidates| candidates.iter().copied().find(|&index| {
                    groups[index].0.iter().eq(key.iter().map(|value| value.as_ref()))
                }));
            if let Some(index) = existing {
                index
            } else {
                if groups.len() >= max_groups {
                    return Err(Error::Execution("GROUP BY group count exceeds memory budget".into()));
                }
                // Drain only on new groups to move computed key payloads
                // while retaining the allocated scratch vector for reuse.
                let owned_key = key.drain(..).map(std::borrow::Cow::into_owned)
                    .collect::<Vec<_>>();
                let bytes = group_key_state_bytes(&owned_key, items.len())?;
                let next_group_bytes = retained_group_bytes.checked_add(bytes)
                    .ok_or_else(|| Error::Execution("GROUP BY key state byte budget exceeded".into()))?;
                next_group_bytes.checked_add(retained_distinct_bytes)
                    .filter(|&size| size <= max_key_state_bytes)
                    .ok_or_else(|| Error::Execution("GROUP BY key state byte budget exceeded".into()))?;
                let index = groups.len();
                groups.push((owned_key, items.iter().map(|item| State::new(&item.op)).collect()));
                group_indices.entry(fingerprint).or_default().push(index);
                retained_group_bytes = next_group_bytes;
                index
            }
        };
        previous_group = Some(index);
        // Even when a group key is cached, every aggregate expression must
        // be evaluated to preserve per-row counts, sums and execution errors.
        for (state, item) in groups[index].1.iter_mut().zip(items) {
            state.update(&item.op, batch, row, &mut retained_distinct_bytes,
                max_key_state_bytes.saturating_sub(retained_group_bytes))?;
        }
    }
    // Move finished states directly into column buffers. Row-major output
    // would allocate a temporary Vec per group and transpose it again.
    let group_count = groups.len();
    let mut columns: Vec<Vec<Value>> = (0..schema.len())
        .map(|_| Vec::with_capacity(group_count)).collect();
    for (mut key, states) in groups {
        for state in states {
            key.push(state.finish()?);
        }
        for (column, value) in columns.iter_mut().zip(key) {
            column.push(value);
        }
    }
    Batch::new_with_row_count(schema, columns, group_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grouped_cardinality_guard_rejects_before_new_group_allocation() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap(),
            (0..4).map(|value| vec![Value::Integer(value)]).collect(),
        ).unwrap();
        let key = [(Expr::Column("id".into()), "key".into())];
        assert!(aggregate_with_max_groups(&input, &key, &[], 2).is_err());
        assert_eq!(aggregate_with_max_groups(&input, &key, &[], 4).unwrap().num_rows(), 4);
        assert!(aggregate_with_max_groups(&input, &[], &[AggregateItem::new(AggregateOp::CountAll, "n")], 0).is_err());
    }

    #[test]
    fn empty_input_count_is_zero_and_sum_is_null() {
        let batch = Batch::empty(
            Schema::new(vec![Field::new("x", DataType::Integer, true)]).unwrap(),
        );
        let result = aggregate(
            &batch,
            &[],
            &[
                AggregateItem::new(AggregateOp::CountAll, "count"),
                AggregateItem::new(AggregateOp::Sum(Expr::Column("x".into())), "sum"),
            ],
        )
        .unwrap();
        assert_eq!(result.rows().unwrap(), vec![vec![Value::Integer(0), Value::Null]]);
    }

    #[test]
    fn group_by_collects_null_keys_and_skips_null_sums() {
        let batch = Batch::from_rows(
            Schema::new(vec![
                Field::new("key", DataType::Text, true),
                Field::new("amount", DataType::Integer, true),
            ])
            .unwrap(),
            vec![
                vec![Value::Null, Value::Integer(2)],
                vec![Value::Null, Value::Null],
                vec![Value::Text("a".into()), Value::Integer(3)],
            ],
        )
        .unwrap();
        let output = aggregate(
            &batch,
            &[(Expr::Column("key".into()), "key".into())],
            &[
                AggregateItem::new(AggregateOp::CountAll, "n"),
                AggregateItem::new(AggregateOp::Sum(Expr::Column("amount".into())), "total"),
            ],
        )
        .unwrap();
        assert_eq!(output.row(0).unwrap(), vec![Value::Null, Value::Integer(2), Value::Integer(2)]);
        assert_eq!(output.row(1).unwrap(), vec![
            Value::Text("a".into()), Value::Integer(1), Value::Integer(3),
        ]);
    }
}

#[cfg(test)]
mod aggregate_nullability_tests {
    use super::*;

    #[test]
    fn counts_are_required_and_sum_remains_nullable() {
        let input = Batch::empty(Schema::new(vec![
            Field::new("value", DataType::Integer, true)
        ]).unwrap());
        let output = aggregate(&input, &[], &[
            AggregateItem::new(AggregateOp::CountAll, "all"),
            AggregateItem::new(AggregateOp::Count(Expr::Column("value".into())), "present"),
            AggregateItem::new(AggregateOp::CountDistinct(Expr::Column("value".into())), "unique"),
            AggregateItem::new(AggregateOp::Sum(Expr::Column("value".into())), "sum"),
        ]).unwrap();
        assert_eq!(output.row(0).unwrap()[0], Value::Integer(0));
        assert!(!output.schema().field(0).unwrap().nullable);
        assert!(!output.schema().field(1).unwrap().nullable);
        assert!(!output.schema().field(2).unwrap().nullable);
        assert!(output.schema().field(3).unwrap().nullable);
    }
}

#[cfg(test)]
mod consolidated_compensated_avg_tests {
    use super::*;
    #[test]
    fn avg_retains_small_contribution_after_large_cancellation() {
        let batch = Batch::from_rows(Schema::new(vec![Field::new("v", DataType::Float, false)]).unwrap(),
            vec![vec![Value::Float(1e16)], vec![Value::Float(1.0)], vec![Value::Float(-1e16)]]).unwrap();
        let output = aggregate(&batch, &[], &[
            AggregateItem::new(AggregateOp::Avg(Expr::Column("v".into())), "avg")
        ]).unwrap();
        let Value::Float(value) = output.value(0, 0).unwrap() else { panic!("AVG result should be FLOAT") };
        assert!((value - 1.0 / 3.0).abs() < 1e-12);
    }
}

#[cfg(test)]
mod aggregate_hash_entry_perf_tests {
    use super::*;

    #[test]
    fn repeated_groups_do_not_consume_admission_budget() {
        let schema = Schema::new(vec![Field::new("s", DataType::Text, true)]).unwrap();
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Text("long repeated string".into())],
            vec![Value::Text("long repeated string".into())],
            vec![Value::Text("other".into())],
            vec![Value::Text("other".into())],
        ]).unwrap();
        let groups = [(Expr::Column("s".into()), "s".into())];
        let agg = [AggregateItem::new(AggregateOp::CountAll, "n")];
        let result = aggregate_with_max_groups(&batch, &groups, &agg, 2).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.column(1).unwrap(), &[Value::Integer(2), Value::Integer(2)]);
        assert!(aggregate_with_max_groups(&batch, &groups, &agg, 1).is_err());
    }
}

#[cfg(test)]
mod bound_group_columns_perf_tests {
    use super::*;
    use crate::expr::BinaryOp;
    #[test]
    fn direct_and_computed_keys_group_nullable_rows() {
        let schema = Schema::new(vec![Field::new("x", DataType::Integer, true)]).unwrap();
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Integer(1)], vec![Value::Integer(1)], vec![Value::Null]
        ]).unwrap();
        let groups = [(Expr::Column("X".into()), "key".into())];
        let result = aggregate_with_max_groups(&batch, &groups,
            &[AggregateItem::new(AggregateOp::CountAll, "count")], 2).unwrap();
        let rows = result.rows().unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&vec![Value::Integer(1), Value::Integer(2)]));
        assert!(rows.contains(&vec![Value::Null, Value::Integer(1)]));
        let computed = Expr::binary(Expr::Column("x".into()),
            BinaryOp::Add, Expr::Literal(Value::Integer(1)));
        assert_eq!(aggregate_with_max_groups(&batch,
            &[(computed, "key".into())], &[], 2).unwrap().num_rows(), 2);
    }
}

#[cfg(test)]
mod global_stream_native_aggregate_tests {
    use super::*;
    #[test]
    fn combines_batches_and_empty_inputs_without_materializing_rows() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let items = [
            AggregateItem::new(AggregateOp::CountAll, "total"),
            AggregateItem::new(AggregateOp::Count(Expr::Column("n".into())), "not_null"),
            AggregateItem::new(AggregateOp::Sum(Expr::Column("n".into())), "sum"),
            AggregateItem::new(AggregateOp::Avg(Expr::Column("n".into())), "avg"),
        ];
        let a = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Null],
        ]).unwrap();
        let b = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(3)], vec![Value::Integer(5)],
        ]).unwrap();
        let result = aggregate_global_stream([Ok(a), Ok(b)].into_iter(),
            &schema, &items, || Ok(())).unwrap();
        assert_eq!(result.row(0).unwrap(), vec![
            Value::Integer(4), Value::Integer(3), Value::Integer(9),
            Value::Float(3.0),
        ]);
        let empty = aggregate_global_stream(std::iter::empty(), &schema,
            &items, || Ok(())).unwrap();
        assert_eq!(empty.row(0).unwrap(), vec![
            Value::Integer(0), Value::Integer(0), Value::Null, Value::Null,
        ]);
    }

    #[test]
    fn catches_unknown_count_columns_and_rejects_unbounded_distinct() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        assert!(global_output_schema(&schema, &[AggregateItem::new(
            AggregateOp::Count(Expr::Column("missing".into())), "n")]).is_err());
        assert!(aggregate_global_stream(std::iter::empty(), &schema,
            &[AggregateItem::new(AggregateOp::CountDistinct(
                Expr::Column("n".into())), "n")], || Ok(())).is_err());
    }
}

#[cfg(test)]
mod count_distinct_budget_regression_tests {
    use super::*;

    #[test]
    fn distinct_keys_are_bounded_and_duplicate_nulls_do_not_use_slots() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap();
        let data = Batch::from_rows(schema,
            std::iter::once(vec![Value::Null])
                .chain((0..65_536).map(|n| vec![Value::Integer(n)]))
                .collect()).unwrap();
        let output = aggregate(&data, &[],
            &[AggregateItem::new(AggregateOp::CountDistinct(Expr::Column("id".into())), "n")]).unwrap();
        assert_eq!(output.row(0).unwrap(), vec![Value::Integer(65_536)]);
        let extra = Batch::from_rows(data.schema().clone(),
            (0..=65_536).map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        assert!(aggregate(&extra, &[],
            &[AggregateItem::new(AggregateOp::CountDistinct(Expr::Column("id".into())), "n")])
            .is_err());
    }

    #[test]
    fn repeated_text_values_count_once_and_nulls_are_ignored() {
        let schema = Schema::new(vec![Field::new("word", DataType::Text, true)]).unwrap();
        let data = Batch::from_rows(schema, vec![
            vec![Value::Text("Berlin".into())],
            vec![Value::Text("Berlin".into())], vec![Value::Null],
        ]).unwrap();
        assert_eq!(aggregate(&data, &[], &[AggregateItem::new(
            AggregateOp::CountDistinct(Expr::Column("word".into())), "n")]).unwrap().row(0).unwrap(),
            vec![Value::Integer(1)]);
    }
}

#[cfg(test)]
mod count_argument_preflight_regressions {
    use super::*;

    #[test]
    fn count_arguments_are_checked_even_on_empty_input() {
        let batch = Batch::empty(Schema::new(vec![
            Field::new("valid", DataType::Text, true)
        ]).unwrap());
        for op in [
            AggregateOp::Count(Expr::Column("missing".into())),
            AggregateOp::CountDistinct(Expr::Column("missing".into())),
        ] {
            assert!(aggregate(&batch, &[], &[AggregateItem::new(op, "count")]).is_err());
        }
        for op in [
            AggregateOp::Count(Expr::Column("valid".into())),
            AggregateOp::CountDistinct(Expr::Column("valid".into())),
            AggregateOp::CountAll,
        ] {
            assert_eq!(aggregate(&batch, &[], &[AggregateItem::new(op, "count")])
                .unwrap().row(0).unwrap(), vec![Value::Integer(0)]);
        }
    }
}

#[cfg(test)]
mod in_memory_group_key_byte_admission_regressions {
    use super::*;

    #[test]
    fn wide_text_groups_are_bounded_even_below_count_limit() {
        let schema = Schema::new(vec![
            Field::new("category", DataType::Text, false),
            Field::new("amount", DataType::Integer, false),
        ]).unwrap();
        let key_a = "a".repeat(400);
        let key_b = "b".repeat(400);
        let input = Batch::from_rows(schema, vec![
            vec![Value::Text(key_a.clone()), Value::Integer(1)],
            vec![Value::Text(key_a.clone()), Value::Integer(2)],
            vec![Value::Text(key_b.clone()), Value::Integer(3)],
        ]).unwrap();
        let group_by = [(Expr::Column("category".into()), "category".into())];
        let items = [AggregateItem::new(
            AggregateOp::Sum(Expr::Column("amount".into())), "total",
        )];
        let one_key = group_key_state_bytes(&[Value::Text(key_a)], items.len()).unwrap();
        assert!(aggregate_with_limits(&input, &group_by, &items, 10, one_key).is_err());
        let result = aggregate_with_limits(&input, &group_by, &items, 10, one_key * 2)
            .unwrap().rows().unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.iter().any(|row| row[1] == Value::Integer(3)));
    }

    #[test]
    fn even_global_aggregates_admit_their_state_budget() {
        let input = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 1).unwrap();
        let items = [AggregateItem::new(AggregateOp::CountAll, "total")];
        assert!(aggregate_with_limits(&input, &[], &items, 1, 0).is_err());
        let exact = group_key_state_bytes(&[], items.len()).unwrap();
        let result = aggregate_with_limits(&input, &[], &items, 1, exact).unwrap();
        assert_eq!(result.row(0).unwrap(), vec![Value::Integer(1)]);
    }
}

#[cfg(test)]
mod borrowed_group_key_regressions {
    use super::*;

    #[test]
    fn repeated_wide_text_groups_and_nulls_preserve_counts_and_order() {
        let schema = Schema::new(vec![
            Field::new("tag", DataType::Text, true),
            Field::new("partition", DataType::Integer, false),
        ]).unwrap();
        let long = "東京".repeat(2048);
        let source = Batch::from_rows(schema, (0..1000).map(|index| vec![
            if index % 3 == 0 { Value::Null } else { Value::Text(long.clone()) },
            Value::Integer(index % 2),
        ]).collect()).unwrap();
        let groups = [
            (Expr::Column("tag".into()), "tag".into()),
            (Expr::Column("partition".into()), "partition".into()),
        ];
        let result = aggregate_with_limits(&source, &groups,
            &[AggregateItem::new(AggregateOp::CountAll, "count")],
            4, 1_000_000).unwrap();
        assert_eq!(result.num_rows(), 4);
        let count = result.column(2).unwrap().iter().map(|value| match value {
            Value::Integer(count) => *count, _ => panic!("COUNT returned wrong type"),
        }).sum::<i64>();
        assert_eq!(count, 1000);
        assert_eq!(result.value(0, 0).unwrap(), &Value::Null);
        assert_eq!(result.value(0, 1).unwrap(), &Value::Integer(0));
        assert!(aggregate_with_limits(&source, &groups,
            &[AggregateItem::new(AggregateOp::CountAll, "count")],
            3, 1_000_000).is_err());
    }

    #[test]
    fn floating_key_hashes_canonicalize_signed_zero_and_nan() {
        let schema = Schema::new(vec![Field::new("key", DataType::Float, true)]).unwrap();
        let source = Batch::from_rows(schema, vec![
            vec![Value::Float(0.0)], vec![Value::Float(-0.0)],
            vec![Value::Float(f64::NAN)], vec![Value::Float(-f64::NAN)],
            vec![Value::Null], vec![Value::Float(1.0)],
        ]).unwrap();
        let result = aggregate_with_limits(&source,
            &[(Expr::Column("key".into()), "key".into())],
            &[AggregateItem::new(AggregateOp::CountAll, "count")],
            4, 4096).unwrap();
        assert_eq!(result.num_rows(), 4);
        assert_eq!(result.column(1).unwrap(),
            &[Value::Integer(2), Value::Integer(2),
              Value::Integer(1), Value::Integer(1)]);
    }
}

#[cfg(test)]
mod reused_group_key_allocation_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn repeated_wide_keys_and_new_groups_keep_exact_counts() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Text, false),
        ]).unwrap();
        let rows = (0..1000).map(|n| vec![
            Value::Text(format!("{}-{}", "🚀".repeat(128), n % 7))
        ]).collect();
        let batch = Batch::from_rows(schema, rows).unwrap();
        let result = aggregate_with_limits(&batch,
            &[(Expr::Column("key".into()), "key".into())],
            &[AggregateItem::new(AggregateOp::CountAll, "hits")],
            7, 1024 * 1024).unwrap();
        assert_eq!(result.num_rows(), 7);
        for index in 0..7 {
            assert_eq!(result.value(index, 1).unwrap(),
                &Value::Integer(if index < 6 { 143 } else { 142 }));
        }
    }
}

#[cfg(test)]
mod aggregate_distinct_global_memory_budget_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    fn numbers(values: &[i64]) -> Batch {
        Batch::from_rows(
            Schema::new(vec![Field::new("value", DataType::Integer, false)]).unwrap(),
            values.iter().map(|&value| vec![Value::Integer(value)]).collect(),
        ).unwrap()
    }

    #[test]
    fn distinct_keys_share_the_same_budget_even_with_one_group() {
        let item = AggregateItem::new(
            AggregateOp::CountDistinct(Expr::Column("value".into())), "distinct");
        let base = group_key_state_bytes(&[], 1).unwrap();
        let entry = distinct_key_footprint(&Value::Integer(1)).unwrap();
        let budget = base + 2 * entry;
        let result = aggregate_with_limits(&numbers(&[1, 1, 2, 2]), &[],
            std::slice::from_ref(&item), 1, budget).unwrap();
        assert_eq!(result.value(0, 0).unwrap(), &Value::Integer(2));
        let error = aggregate_with_limits(&numbers(&[1, 2, 3]), &[],
            &[item], 1, budget).err().unwrap();
        assert!(error.to_string().contains("DISTINCT total byte budget"));
    }

    #[test]
    fn different_distinct_aggregate_states_charge_separately() {
        let items = [
            AggregateItem::new(AggregateOp::CountDistinct(Expr::Column("value".into())), "count"),
            AggregateItem::new(AggregateOp::SumDistinct(Expr::Column("value".into())), "sum"),
            AggregateItem::new(AggregateOp::AvgDistinct(Expr::Column("value".into())), "avg"),
        ];
        let base = group_key_state_bytes(&[], items.len()).unwrap();
        let entry = distinct_key_footprint(&Value::Integer(8)).unwrap();
        let input = numbers(&[8, 8, 8]);
        let result = aggregate_with_limits(&input, &[], &items, 1, base + 3 * entry).unwrap();
        assert_eq!(result.row(0).unwrap(),
            vec![Value::Integer(1), Value::Integer(8), Value::Float(8.0)]);
        assert!(aggregate_with_limits(&input, &[], &items, 1, base + 2 * entry).is_err());
    }

    #[test]
    fn group_keys_and_distinct_entries_share_one_retained_limit() {
        let schema = Schema::new(vec![
            Field::new("bucket", DataType::Integer, false),
            Field::new("value", DataType::Integer, false),
        ]).unwrap();
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
        ]).unwrap();
        let group_by = [(Expr::Column("bucket".into()), "bucket".into())];
        let item = [AggregateItem::new(
            AggregateOp::CountDistinct(Expr::Column("value".into())), "count")];
        let group_size = group_key_state_bytes(&[Value::Integer(1)], 1).unwrap();
        let entry = distinct_key_footprint(&Value::Integer(10)).unwrap();
        // Enough for the groups and just one DISTINCT value. The second
        // group must not bypass accounting of the first group's hash set.
        assert!(aggregate_with_limits(&batch, &group_by, &item, 2,
            2 * group_size + entry).is_err());
        let result = aggregate_with_limits(&batch, &group_by, &item, 2,
            2 * (group_size + entry)).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.row(0).unwrap(), vec![Value::Integer(1), Value::Integer(1)]);
        assert_eq!(result.row(1).unwrap(), vec![Value::Integer(2), Value::Integer(1)]);
    }
}

#[cfg(test)]
mod clustered_groupby_key_run_regressions {
    use super::*;
    use crate::expr::BinaryOp;

    #[test]
    fn composite_nullable_float_and_unicode_runs_update_all_aggregate_states() {
        let schema = Schema::new(vec![
            Field::new("number", DataType::Float, true),
            Field::new("word", DataType::Text, true),
            Field::new("amount", DataType::Integer, false),
        ]).unwrap();
        let wide = "🙂é".repeat(4096);
        let mut rows = Vec::new();
        for (number, word) in [
            (Value::Null, Value::Null),
            (Value::Float(-0.0), Value::Text(wide.clone())),
            (Value::Float(0.0), Value::Text(wide.clone())),
            (Value::Float(f64::NAN), Value::Text("nan".into())),
            (Value::Float(-f64::NAN), Value::Text("nan".into())),
            (Value::Float(1.0), Value::Text("one".into())),
            (Value::Float(2.0), Value::Null),
        ] {
            for index in 0..40 {
                rows.push(vec![number.clone(), word.clone(),
                    Value::Integer((index % 3) + 1)]);
            }
        }
        let batch = Batch::from_rows(schema, rows).unwrap();
        let keys = [
            (Expr::Column("number".into()), "number".into()),
            (Expr::Column("word".into()), "word".into()),
        ];
        let items = [
            AggregateItem::new(AggregateOp::CountAll, "rows"),
            AggregateItem::new(AggregateOp::Sum(Expr::Column("amount".into())), "total"),
            AggregateItem::new(AggregateOp::CountDistinct(
                Expr::Column("amount".into())), "distinct"),
        ];
        let output = aggregate_with_max_groups(&batch, &keys, &items, 5).unwrap();
        let part_sum = (0..40).map(|index| (index % 3) + 1).sum::<i64>();
        assert_eq!(output.rows().unwrap(), vec![
            vec![Value::Null, Value::Null, Value::Integer(40),
                Value::Integer(part_sum), Value::Integer(3)],
            vec![Value::Float(-0.0), Value::Text(wide),
                Value::Integer(80), Value::Integer(2 * part_sum), Value::Integer(3)],
            vec![Value::Float(f64::NAN), Value::Text("nan".into()),
                Value::Integer(80), Value::Integer(2 * part_sum), Value::Integer(3)],
            vec![Value::Float(1.0), Value::Text("one".into()),
                Value::Integer(40), Value::Integer(part_sum), Value::Integer(3)],
            vec![Value::Float(2.0), Value::Null,
                Value::Integer(40), Value::Integer(part_sum), Value::Integer(3)],
        ]);
        assert!(aggregate_with_max_groups(&batch, &keys, &items, 4).is_err());
    }

    #[test]
    fn cached_group_does_not_hide_failing_per_row_expression() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Integer, false),
            Field::new("denominator", DataType::Integer, false),
        ]).unwrap();
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(1), Value::Integer(0)],
        ]).unwrap();
        let items = [AggregateItem::new(AggregateOp::Sum(Expr::binary(
            Expr::Literal(Value::Integer(10)), BinaryOp::Divide,
            Expr::Column("denominator".into()),
        )), "sum")];
        assert!(aggregate(&batch, &[(Expr::Column("key".into()), "key".into())], &items).is_err());
    }
}

#[cfg(test)]
mod direct_aggregate_expression_budget_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn public_aggregate_apis_preflight_aggregate_and_grouping_trees() {
        let schema = Schema::new(vec![Field::new("ok", DataType::Boolean, false)]).unwrap();
        let batch = Batch::from_rows(schema, vec![vec![Value::Boolean(true)]]).unwrap();
        let mut deep = Expr::Column("ok".into());
        for _ in 0..80 { deep = Expr::Not(Box::new(deep)); }
        let items = [AggregateItem::new(AggregateOp::Count(deep.clone()), "count")];
        assert!(global_output_schema(batch.schema(), &items).unwrap_err().to_string()
            .contains("complexity budget"));
        assert!(aggregate(&batch, &[], &items).unwrap_err().to_string()
            .contains("complexity budget"));
        let group = vec![(deep, "group".into())];
        let rows = [AggregateItem::new(AggregateOp::CountAll, "n")];
        assert!(aggregate(&batch, &group, &rows).unwrap_err().to_string()
            .contains("complexity budget"));
    }
}

#[cfg(test)]
mod grouped_direct_column_output_regressions {
    use super::*;

    #[test]
    fn multiple_groups_preserve_nullable_keys_order_and_aggregates() {
        let schema = Schema::new(vec![
            Field::new("category", DataType::Text, true),
            Field::new("amount", DataType::Integer, false),
        ]).unwrap();
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Text("世界".into()), Value::Integer(2)],
            vec![Value::Null, Value::Integer(3)],
            vec![Value::Text("世界".into()), Value::Integer(4)],
            vec![Value::Text("other".into()), Value::Integer(5)],
        ]).unwrap();
        let groups = [(Expr::Column("category".into()), "category".into())];
        let items = [
            AggregateItem::new(AggregateOp::CountAll, "n"),
            AggregateItem::new(AggregateOp::Sum(Expr::Column("amount".into())), "total"),
        ];
        let output = aggregate_with_limits(&batch, &groups, &items, 10, 4096).unwrap();
        assert_eq!(output.rows().unwrap(), vec![
            vec![Value::Text("世界".into()), Value::Integer(2), Value::Integer(6)],
            vec![Value::Null, Value::Integer(1), Value::Integer(3)],
            vec![Value::Text("other".into()), Value::Integer(1), Value::Integer(5)],
        ]);
    }
}

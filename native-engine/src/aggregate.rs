use std::collections::{HashMap, HashSet};

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
    Avg(Expr),
    Min(Expr),
    Max(Expr),
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
            AggregateOp::Sum(expr) => {
                let kind = expr.output_type(schema)?;
                match kind {
                    DataType::Integer | DataType::Float => Ok(kind),
                    _ => Err(Error::Type("SUM requires a numeric column".into())),
                }
            }
            AggregateOp::Avg(expr) => {
                match expr.output_type(schema)? {
                    DataType::Integer | DataType::Float => Ok(DataType::Float),
                    _ => Err(Error::Type("AVG requires a numeric column".into())),
                }
            }
            AggregateOp::Min(expr) | AggregateOp::Max(expr) => expr.output_type(schema),
        }
    }
}

const MAX_COUNT_DISTINCT_KEYS: usize = 1_000_000;

fn insert_count_distinct(values: &mut HashSet<Value>, value: Value, limit: usize) -> Result<()> {
    if values.len() >= limit && !values.contains(&value) {
        return Err(Error::Execution("COUNT DISTINCT key budget exceeded".into()));
    }
    values.insert(value);
    Ok(())
}

#[derive(Clone)]
enum State {
    Count(i64),
    CountDistinct(HashSet<Value>),
    Sum(Option<Value>),
    Avg { sum: f64, count: i64 },
    Min(Option<Value>),
    Max(Option<Value>),
}

impl State {
    fn new(op: &AggregateOp) -> Self {
        match op {
            AggregateOp::CountAll | AggregateOp::Count(_) => Self::Count(0),
            AggregateOp::CountDistinct(_) => Self::CountDistinct(HashSet::new()),
            AggregateOp::Sum(_) => Self::Sum(None),
            AggregateOp::Avg(_) => Self::Avg { sum: 0.0, count: 0 },
            AggregateOp::Min(_) => Self::Min(None),
            AggregateOp::Max(_) => Self::Max(None),
        }
    }

    fn update(&mut self, op: &AggregateOp, batch: &Batch, row: usize) -> Result<()> {
        let value = match op {
            AggregateOp::CountAll => Value::Integer(1),
            AggregateOp::Count(expr)
            | AggregateOp::CountDistinct(expr)
            | AggregateOp::Sum(expr)
            | AggregateOp::Avg(expr)
            | AggregateOp::Min(expr)
            | AggregateOp::Max(expr) => expr.evaluate(batch, row)?,
        };
        match self {
            Self::Count(count) => {
                if !value.is_null() {
                    *count = count
                        .checked_add(1)
                        .ok_or_else(|| Error::Execution("COUNT overflow".into()))?;
                }
            }
            Self::CountDistinct(values) => {
                if !value.is_null() {
                    insert_count_distinct(values, value, MAX_COUNT_DISTINCT_KEYS)?;
                }
            }
            Self::Sum(total) => {
                if value.is_null() {
                    return Ok(());
                }
                let updated = match (total.as_ref(), &value) {
                    (None, Value::Integer(_) | Value::Float(_)) => value.clone(),
                    (Some(Value::Integer(a)), Value::Integer(b)) => Value::Integer(
                        a.checked_add(*b)
                            .ok_or_else(|| Error::Execution("SUM overflow".into()))?,
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
            }
            Self::Avg { sum, count } => {
                if !value.is_null() {
                    let number = match value {
                        Value::Integer(value) => value as f64,
                        Value::Float(value) => value,
                        _ => return Err(Error::Type("AVG requires numeric values".into())),
                    };
                    let next = *sum + number;
                    if !next.is_finite() {
                        return Err(Error::Execution("AVG overflow".into()));
                    }
                    *count = count.checked_add(1)
                        .ok_or_else(|| Error::Execution("AVG count overflow".into()))?;
                    *sum = next;
                }
            }
            Self::Min(minimum) => update_extreme(minimum, value, true)?,
            Self::Max(maximum) => update_extreme(maximum, value, false)?,
        }
        Ok(())
    }

    fn finish(self) -> Result<Value> {
        Ok(match self {
            Self::Count(value) => Value::Integer(value),
            Self::CountDistinct(values) => Value::Integer(
                i64::try_from(values.len())
                    .map_err(|_| Error::Execution("COUNT DISTINCT overflow".into()))?
            ),
            Self::Avg { sum, count } =>
                if count == 0 { Value::Null } else { Value::Float(sum / count as f64) },
            Self::Sum(value) | Self::Min(value) | Self::Max(value) => {
                value.unwrap_or(Value::Null)
            }
        })
    }
}

fn update_extreme(current: &mut Option<Value>, value: Value, minimum: bool) -> Result<()> {
    if value.is_null() {
        return Ok(());
    }
    if let Some(existing) = current {
        let comparison = value
            .sql_compare(existing)?
            .ok_or_else(|| Error::Type("MIN/MAX cannot compare NaN".into()))?;
        if (minimum && comparison.is_lt()) || (!minimum && comparison.is_gt()) {
            *existing = value;
        }
    } else {
        *current = Some(value);
    }
    Ok(())
}

pub fn aggregate(
    batch: &Batch,
    group_by: &[(Expr, String)],
    items: &[AggregateItem],
) -> Result<Batch> {
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
    let mut group_indices: HashMap<Vec<Value>, usize> = HashMap::new();
    let mut groups: Vec<(Vec<Value>, Vec<State>)> = Vec::new();

    if batch.num_rows() == 0 && group_by.is_empty() {
        groups.push((Vec::new(), items.iter().map(|item| State::new(&item.op)).collect()));
    }
    for row in 0..batch.num_rows() {
        let key = group_by
            .iter()
            .map(|(expr, _)| expr.evaluate(batch, row))
            .collect::<Result<Vec<_>>>()?;
        let index = if let Some(index) = group_indices.get(&key) {
            *index
        } else {
            let index = groups.len();
            let _ = group_indices.insert(key.clone(), index);
            groups.push((key, items.iter().map(|item| State::new(&item.op)).collect()));
            index
        };
        for (state, item) in groups[index].1.iter_mut().zip(items) {
            state.update(&item.op, batch, row)?;
        }
    }
    let rows = groups
        .into_iter()
        .map(|(mut key, states)| {
            key.extend(states.into_iter().map(State::finish).collect::<Result<Vec<_>>>()?);
            Ok(key)
        })
        .collect::<Result<Vec<_>>>()?;
    Batch::from_rows(schema, rows)
}

#[cfg(test)]
mod tests {
    use super::*;

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
mod count_column_type_regression {
    use super::*;
    #[test]
    fn missing_count_arguments_are_rejected_with_zero_input_rows() {
        let data = Batch::empty(Schema::new(vec![
            Field::new("x", DataType::Integer, true),
        ]).unwrap());
        for operation in [
            AggregateOp::Count(Expr::Column("missing".into())),
            AggregateOp::CountDistinct(Expr::Column("missing".into())),
        ] {
            assert!(aggregate(&data, &[], &[AggregateItem::new(operation, "n")]).is_err());
        }
        let valid = aggregate(&data, &[], &[
            AggregateItem::new(AggregateOp::Count(Expr::Column("x".into())), "n"),
        ]).unwrap();
        assert_eq!(valid.row(0).unwrap(), vec![Value::Integer(0)]);
    }
}

#[cfg(test)]
mod bounded_count_distinct_regression {
    use super::*;
    #[test]
    fn distinct_budget_counts_only_new_keys_and_ignores_duplicates() {
        let mut keys = HashSet::new();
        insert_count_distinct(&mut keys, Value::Integer(1), 2).unwrap();
        insert_count_distinct(&mut keys, Value::Integer(2), 2).unwrap();
        insert_count_distinct(&mut keys, Value::Integer(2), 2).unwrap();
        assert!(insert_count_distinct(&mut keys, Value::Integer(3), 2).is_err());
        assert_eq!(keys.len(), 2);
    }
}

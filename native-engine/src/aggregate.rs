use std::collections::HashMap;

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
            AggregateOp::CountAll | AggregateOp::Count(_) => Ok(DataType::Integer),
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

#[derive(Clone)]
enum State {
    Count(i64),
    Sum(Option<Value>),
    Avg { sum: f64, count: i64 },
    Min(Option<Value>),
    Max(Option<Value>),
}

impl State {
    fn new(op: &AggregateOp) -> Self {
        match op {
            AggregateOp::CountAll | AggregateOp::Count(_) => Self::Count(0),
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

    fn finish(self) -> Value {
        match self {
            Self::Count(value) => Value::Integer(value),
            Self::Avg { sum, count } =>
                if count == 0 { Value::Null } else { Value::Float(sum / count as f64) },
            Self::Sum(value) | Self::Min(value) | Self::Max(value) => {
                value.unwrap_or(Value::Null)
            }
        }
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
        fields.push(Field::new(&item.alias, item.output_type(batch.schema())?, true));
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
            key.extend(states.into_iter().map(State::finish));
            key
        })
        .collect();
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

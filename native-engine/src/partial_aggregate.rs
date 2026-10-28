//! Mergeable partition-local hash aggregation for native analytic stages.
use std::collections::HashMap;

use crate::{
    aggregate::{self, AggregateItem, AggregateOp},
    batch::Batch,
    expr::Expr,
    scalar::Value,
    Error, Result,
};

fn combine(old: &Value, new: &Value, operation: &AggregateOp) -> Result<Value> {
    match operation {
        AggregateOp::CountAll | AggregateOp::Count(_) => match (old, new) {
            (Value::Integer(a), Value::Integer(b)) => a.checked_add(*b)
                .map(Value::Integer)
                .ok_or_else(|| Error::Execution("partial COUNT overflow".into())),
            _ => Err(Error::Type("invalid partial COUNT state".into())),
        },
        AggregateOp::Sum(_) => match (old, new) {
            (Value::Null, _) => Ok(new.clone()),
            (_, Value::Null) => Ok(old.clone()),
            (Value::Integer(a), Value::Integer(b)) => a.checked_add(*b)
                .map(Value::Integer)
                .ok_or_else(|| Error::Execution("partial SUM overflow".into())),
            (Value::Float(a), Value::Float(b)) => float_sum(*a + *b),
            (Value::Float(a), Value::Integer(b)) => float_sum(*a + *b as f64),
            (Value::Integer(a), Value::Float(b)) => float_sum(*a as f64 + *b),
            _ => Err(Error::Type("invalid partial SUM state".into())),
        },
        AggregateOp::Min(_) | AggregateOp::Max(_) => {
            if old.is_null() { return Ok(new.clone()); }
            if new.is_null() { return Ok(old.clone()); }
            let comparison = new.sql_compare(old)?
                .ok_or_else(|| Error::Type("incomparable partial MIN/MAX".into()))?;
            let take_new = match operation {
                AggregateOp::Min(_) => comparison.is_lt(),
                AggregateOp::Max(_) => comparison.is_gt(),
                _ => unreachable!(),
            };
            Ok(if take_new { new.clone() } else { old.clone() })
        }
    }
}

fn float_sum(result: f64) -> Result<Value> {
    if result.is_finite() {
        Ok(Value::Float(result))
    } else {
        Err(Error::Execution("partial SUM overflow".into()))
    }
}

/// Compute and merge local aggregate states without concatenating inputs.
/// COUNT states sum counts; SUM, MIN, and MAX combine their partial values.
pub fn aggregate_partitions(
    partitions: &[Batch],
    group_by: &[(Expr, String)],
    items: &[AggregateItem],
) -> Result<Batch> {
    let first = partitions.first()
        .ok_or_else(|| Error::Schema("partial aggregation needs an input schema".into()))?;
    let mut groups: Vec<(Vec<Value>, Vec<Value>)> = Vec::new();
    let mut positions = HashMap::<Vec<Value>, usize>::new();
    let mut schema = None;
    for partition in partitions {
        if partition.schema() != first.schema() {
            return Err(Error::Schema("partial aggregate schema mismatch".into()));
        }
        let partial = aggregate::aggregate(partition, group_by, items)?;
        if schema.is_none() { schema = Some(partial.schema().clone()); }
        for row in partial.rows()? {
            let (key, values) = row.split_at(group_by.len());
            if let Some(&index) = positions.get(key) {
                for (position, item) in items.iter().enumerate() {
                    groups[index].1[position] =
                        combine(&groups[index].1[position], &values[position], &item.op)?;
                }
            } else {
                let index = groups.len();
                let key = key.to_vec();
                positions.insert(key.clone(), index);
                groups.push((key, values.to_vec()));
            }
        }
    }
    let rows = groups.into_iter().map(|(mut key, values)| {
        key.extend(values);
        key
    }).collect();
    Batch::from_rows(schema.expect("at least one partition"), rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{schema::{DataType, Field, Schema}, scalar::Value};

    #[test]
    fn grouped_partials_match_single_global_aggregation() {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Text, true),
            Field::new("revenue", DataType::Integer, true),
        ]).unwrap();
        let rows = vec![
            vec![Value::Text("A".into()), Value::Integer(10)],
            vec![Value::Null, Value::Integer(5)],
            vec![Value::Text("A".into()), Value::Integer(2)],
            vec![Value::Null, Value::Null],
        ];
        let parts = vec![
            Batch::from_rows(schema.clone(), rows[..2].to_vec()).unwrap(),
            Batch::from_rows(schema.clone(), rows[2..].to_vec()).unwrap(),
        ];
        let by = vec![(Expr::Column("city".into()), "city".into())];
        let items = vec![
            AggregateItem::new(AggregateOp::CountAll, "n"),
            AggregateItem::new(AggregateOp::Sum(Expr::Column("revenue".into())), "s"),
            AggregateItem::new(AggregateOp::Max(Expr::Column("revenue".into())), "max"),
        ];
        let expected = aggregate::aggregate(
            &Batch::from_rows(schema, rows).unwrap(), &by, &items,
        ).unwrap();
        let actual = aggregate_partitions(&parts, &by, &items).unwrap();
        let mut expected_rows = expected.rows().unwrap().into_iter()
            .map(|row| format!("{row:?}")).collect::<Vec<_>>();
        let mut actual_rows = actual.rows().unwrap().into_iter()
            .map(|row| format!("{row:?}")).collect::<Vec<_>>();
        expected_rows.sort();
        actual_rows.sort();
        assert_eq!(expected_rows, actual_rows);
    }

    #[test]
    fn empty_partitioned_global_count_is_zero() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let parts = vec![Batch::empty(schema.clone()), Batch::empty(schema)];
        let result = aggregate_partitions(
            &parts, &[], &[AggregateItem::new(AggregateOp::CountAll, "count")],
        ).unwrap();
        assert_eq!(result.row(0).unwrap(), vec![Value::Integer(0)]);
    }
}

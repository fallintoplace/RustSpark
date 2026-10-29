use std::cmp::Ordering;

use crate::{
    batch::Batch,
    expr::Expr,
    scalar::Value,
    Result,
};

#[derive(Clone, Debug, PartialEq)]
pub struct SortKey {
    pub expression: Expr,
    pub descending: bool,
    pub nulls_first: bool,
}

impl SortKey {
    pub fn ascending(expression: Expr) -> Self {
        Self {
            expression,
            descending: false,
            nulls_first: true,
        }
    }
}

pub(crate) fn compare_values(left: &Value, right: &Value, descending: bool, nulls_first: bool) -> Ordering {
    match (left.is_null(), right.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => {
            if nulls_first { Ordering::Less } else { Ordering::Greater }
        }
        (false, true) => {
            if nulls_first { Ordering::Greater } else { Ordering::Less }
        }
        (false, false) => {
            // SQL ORDER BY requires a total order even when predicates use
            // unordered comparisons. Put NaN after all non-NULL numeric values.
            let ordering = match (left, right) {
                (Value::Float(a), Value::Float(b)) if a.is_nan() || b.is_nan() =>
                    a.is_nan().cmp(&b.is_nan()),
                (Value::Float(a), _) if a.is_nan() => Ordering::Greater,
                (_, Value::Float(b)) if b.is_nan() => Ordering::Less,
                _ => left.sql_compare(right).ok().flatten().unwrap_or(Ordering::Equal),
            };
            if descending { ordering.reverse() } else { ordering }
        }
    }
}

pub fn sort(batch: &Batch, keys: &[SortKey]) -> Result<Batch> {
    if keys.is_empty() {
        return Ok(batch.clone());
    }
    for key in keys {
        key.expression.output_type(batch.schema())?;
    }
    let mut decorated = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let values = keys
            .iter()
            .map(|key| key.expression.evaluate(batch, row))
            .collect::<Result<Vec<_>>>()?;
        decorated.push((row, values));
    }
    decorated.sort_by(|(_, left), (_, right)| {
        for ((a, b), key) in left.iter().zip(right).zip(keys) {
            let comparison = compare_values(a, b, key.descending, key.nulls_first);
            if comparison != Ordering::Equal {
                return comparison;
            }
        }
        Ordering::Equal
    });
    batch.take(&decorated.into_iter().map(|(row, _)| row).collect::<Vec<_>>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        scalar::Value,
        schema::{DataType, Field, Schema},
    };

    #[test]
    fn nulls_and_ties_have_deterministic_order() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("x", DataType::Integer, true)]).unwrap(),
            vec![
                vec![Value::Integer(2)],
                vec![Value::Null],
                vec![Value::Integer(1)],
                vec![Value::Integer(2)],
            ],
        )
        .unwrap();
        let result = sort(&batch, &[SortKey::ascending(Expr::Column("x".into()))]).unwrap();
        assert_eq!(result.rows().unwrap(), vec![
            vec![Value::Null],
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(2)],
        ]);
    }
}

#[cfg(test)]
mod nan_sort_engine_tests {
    use super::*;
    use crate::{schema::{DataType, Field, Schema}, topk};

    #[test]
    fn all_native_sort_paths_place_nan_after_finite_values() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("score", DataType::Float, true)]).unwrap(),
            vec![vec![Value::Float(2.0)], vec![Value::Float(f64::NAN)],
                 vec![Value::Null], vec![Value::Float(-1.0)],
                 vec![Value::Float(0.0)]],
        ).unwrap();
        let keys = vec![SortKey {
            expression: Expr::Column("score".into()), descending: false, nulls_first: false,
        }];
        let sorted = sort(&input, &keys).unwrap();
        assert_eq!(sorted.value(0, 0).unwrap(), &Value::Float(-1.0));
        assert_eq!(sorted.value(1, 0).unwrap(), &Value::Float(0.0));
        assert_eq!(sorted.value(2, 0).unwrap(), &Value::Float(2.0));
        assert!(matches!(sorted.value(3, 0).unwrap(), Value::Float(v) if v.is_nan()));
        assert_eq!(sorted.value(4, 0).unwrap(), &Value::Null);
        assert_eq!(topk::top_k(&input, &keys, 3).unwrap().rows().unwrap(),
            sorted.slice_rows(0, 3).unwrap().rows().unwrap());
    }
}

#[cfg(test)]
mod sort_key_preflight_regression {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn sort_checks_keys_even_without_input_rows() {
        let batch = Batch::empty(Schema::new(vec![
            Field::new("x", DataType::Integer, false),
        ]).unwrap());
        assert!(sort(&batch, &[SortKey::ascending(Expr::Column("missing".into()))]).is_err());
        assert_eq!(sort(&batch, &[SortKey::ascending(Expr::Column("x".into()))])
            .unwrap().num_rows(), 0);
    }
}

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

fn compare_values(left: &Value, right: &Value, descending: bool, nulls_first: bool) -> Ordering {
    match (left.is_null(), right.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => {
            if nulls_first { Ordering::Less } else { Ordering::Greater }
        }
        (false, true) => {
            if nulls_first { Ordering::Greater } else { Ordering::Less }
        }
        (false, false) => {
            let ordering = left.sql_compare(right).ok().flatten().unwrap_or(Ordering::Equal);
            if descending { ordering.reverse() } else { ordering }
        }
    }
}

pub fn sort(batch: &Batch, keys: &[SortKey]) -> Result<Batch> {
    if keys.is_empty() {
        return Ok(batch.clone());
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

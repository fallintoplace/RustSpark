//! Top-K selection using O(k) candidate memory instead of a full sort.
use std::{cmp::Ordering, collections::BinaryHeap, sync::Arc};
use crate::{batch::Batch, scalar::Value, sort::SortKey, Result};

#[derive(Clone, Debug)]
struct Candidate {
    row: usize,
    values: Vec<Value>,
    options: Arc<Vec<(bool, bool)>>,
}

fn compare(left: &Candidate, right: &Candidate) -> Ordering {
    for ((a, b), (descending, nulls_first)) in left.values.iter()
        .zip(&right.values).zip(left.options.iter()) {
        let order = match (a.is_null(), b.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => if *nulls_first { Ordering::Less } else { Ordering::Greater },
            (false, true) => if *nulls_first { Ordering::Greater } else { Ordering::Less },
            (false, false) => {
                let order = a.sql_compare(b).ok().flatten().unwrap_or(Ordering::Equal);
                if *descending { order.reverse() } else { order }
            }
        };
        if order != Ordering::Equal { return order; }
    }
    left.row.cmp(&right.row)
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool { compare(self, other) == Ordering::Equal }
}
impl Eq for Candidate {}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering { compare(self, other) }
}

pub fn top_k(batch: &Batch, keys: &[SortKey], k: usize) -> Result<Batch> {
    if k == 0 { return batch.take(&[]); }
    let options = Arc::new(keys.iter().map(|k| (k.descending, k.nulls_first)).collect());
    let mut heap = BinaryHeap::with_capacity(k.min(batch.num_rows()));
    for row in 0..batch.num_rows() {
        let values = keys.iter().map(|key| key.expression.evaluate(batch, row))
            .collect::<Result<Vec<_>>>()?;
        let candidate = Candidate { row, values, options: Arc::clone(&options) };
        if heap.len() < k {
            heap.push(candidate);
        } else if let Some(worst) = heap.peek() {
            if candidate < *worst {
                heap.pop();
                heap.push(candidate);
            }
        }
    }
    let mut retained = heap.into_vec();
    retained.sort();
    batch.take(&retained.into_iter().map(|row| row.row).collect::<Vec<_>>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{expr::Expr, scalar::Value, schema::{DataType, Field, Schema}, sort};

    #[test]
    fn topk_matches_stable_full_sort_with_nulls_and_ties() {
        let batch = Batch::from_rows(
            Schema::new(vec![
                Field::new("score", DataType::Integer, true),
                Field::new("id", DataType::Integer, false)
            ]).unwrap(),
            vec![
                vec![Value::Integer(3), Value::Integer(1)],
                vec![Value::Integer(1), Value::Integer(2)],
                vec![Value::Null, Value::Integer(3)],
                vec![Value::Integer(3), Value::Integer(4)],
                vec![Value::Integer(2), Value::Integer(5)],
            ]
        ).unwrap();
        let keys = vec![SortKey {expression: Expr::Column("score".into()), descending: true, nulls_first: false}];
        let sorted = sort::sort(&batch, &keys).unwrap();
        for limit in [0, 1, 2, 4, 10] {
            let expected = sorted.take(&(0..limit.min(sorted.num_rows())).collect::<Vec<_>>()).unwrap();
            assert_eq!(top_k(&batch, &keys, limit).unwrap(), expected);
        }
    }
}

 
/// Keep only OFFSET + LIMIT candidates, preserving stable ties.
pub fn top_k_offset(batch: &Batch, keys: &[SortKey], offset: usize, count: usize) -> Result<Batch> {
    if count == 0 {
        return batch.take(&[]);
    }
    let required = offset.checked_add(count)
        .ok_or_else(|| crate::Error::Execution("top-K pagination overflow".into()))?;
    let best = top_k(batch, keys, required)?;
    crate::limit::limit(&best, offset, Some(count))
}

#[cfg(test)]
mod topk_offset_feature_tests {
    use super::*;
    use crate::{expr::Expr, scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn pagination_matches_stable_sorted_rows() {
        let schema = Schema::new(vec![Field::new("score", DataType::Integer, false)]).unwrap();
        let input = Batch::from_rows(schema, vec![
            vec![Value::Integer(5)], vec![Value::Integer(2)],
            vec![Value::Integer(9)], vec![Value::Integer(2)],
        ]).unwrap();
        let keys = vec![SortKey::ascending(Expr::Column("score".into()))];
        assert_eq!(top_k_offset(&input, &keys, 1, 2).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(2)], vec![Value::Integer(5)]]);
        assert_eq!(top_k_offset(&input, &keys, 10, 2).unwrap().num_rows(), 0);
        assert!(top_k_offset(&input, &keys, usize::MAX, 2).is_err());
    }
}


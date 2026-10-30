//! Top-K selection using O(k) candidate memory instead of a full sort.
use std::{cmp::Ordering, collections::BinaryHeap, sync::Arc};
use crate::{batch::Batch, expr::Expr, scalar::Value, sort::SortKey, Result};

#[derive(Clone, Debug)]
struct Candidate {
    row: usize,
    values: Vec<Value>,
    options: Arc<Vec<(bool, bool)>>,
}

fn compare(left: &Candidate, right: &Candidate) -> Ordering {
    for ((a, b), (descending, nulls_first)) in left.values.iter()
        .zip(&right.values).zip(left.options.iter()) {
        let order = crate::sort::compare_values(a, b, *descending, *nulls_first);
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

struct ColumnCandidate<'a> {
    row: usize,
    columns: &'a [&'a [Value]],
    options: &'a [(bool, bool)],
}

impl PartialEq for ColumnCandidate<'_> {
    fn eq(&self, other: &Self) -> bool { self.cmp(other) == Ordering::Equal }
}
impl Eq for ColumnCandidate<'_> {}
impl PartialOrd for ColumnCandidate<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
impl Ord for ColumnCandidate<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        for (column, (descending, nulls_first)) in self.columns.iter().zip(self.options.iter()) {
            let comparison = crate::sort::compare_values(
                &column[self.row], &column[other.row], *descending, *nulls_first,
            );
            if comparison != Ordering::Equal { return comparison; }
        }
        self.row.cmp(&other.row)
    }
}

fn top_k_columns(
    batch: &Batch, keys: &[SortKey], k: usize,
    check: &mut impl FnMut() -> Result<()>,
) -> Result<Batch> {
    let columns = keys.iter().map(|key| match &key.expression {
        crate::expr::Expr::Column(name) => batch.column(batch.schema().column(name)?),
        _ => unreachable!("column-only Top-K"),
    }).collect::<Result<Vec<_>>>()?;
    let options = keys.iter().map(|key| (key.descending, key.nulls_first)).collect::<Vec<_>>();
    let mut heap = BinaryHeap::with_capacity(k);
    for row in 0..batch.num_rows() {
        if row % 1024 == 0 { check()?; }
        let candidate = ColumnCandidate { row, columns: &columns, options: &options };
        if heap.len() < k {
            heap.push(candidate);
        } else if let Some(mut worst) = heap.peek_mut() {
            if candidate < *worst { *worst = candidate; }
        }
    }
    let mut retained = heap.into_vec();
    retained.sort();
    batch.take(&retained.into_iter().map(|candidate| candidate.row).collect::<Vec<_>>())
}

pub fn top_k(batch: &Batch, keys: &[SortKey], k: usize) -> Result<Batch> {
    top_k_checked(batch, keys, k, || Ok(()))
}

pub fn top_k_checked(
    batch: &Batch, keys: &[SortKey], k: usize,
    mut check: impl FnMut() -> Result<()>,
) -> Result<Batch> {
    check()?;
    for key in keys {
        key.expression.output_type(batch.schema())?;
    }
    if k == 0 { return batch.take(&[]); }
    if k >= batch.num_rows() && !keys.is_empty() {
        let sorted = crate::sort::sort(batch, keys)?;
        check()?;
        return Ok(sorted);
    }
    if keys.is_empty() {
        return batch.slice_rows(0, k.min(batch.num_rows()));
    }
    if k == 1 && keys.len() == 1 {
        if let Expr::Column(name) = &keys[0].expression {
            // For a one-row result the heap, sort options, and candidate
            // vectors are unnecessary. Scan the borrowed key column once.
            let column = batch.column(batch.schema().column(name)?)?;
            let mut best = 0usize;
            for row in 1..batch.num_rows() {
                if row % 1024 == 0 { check()?; }
                if crate::sort::compare_values(
                    &column[row], &column[best], keys[0].descending, keys[0].nulls_first,
                ) == Ordering::Less {
                    best = row;
                }
            }
            return batch.take(&[best]);
        }
    }
    if keys.iter().all(|key| matches!(&key.expression, crate::expr::Expr::Column(_))) {
        return top_k_columns(batch, keys, k, &mut check);
    }
    let options = Arc::new(keys.iter().map(|k| (k.descending, k.nulls_first)).collect());
    let key_columns = keys.iter().map(|key| match &key.expression {
        Expr::Column(name) => {
            let index = batch.schema().column(name)?;
            Ok(Some(batch.column(index)?))
        }
        _ => Ok(None),
    }).collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::with_capacity(k.min(batch.num_rows()));
    for row in 0..batch.num_rows() {
        if row % 1024 == 0 { check()?; }
        let values = keys.iter().zip(&key_columns).map(|(key, column)| {
            match column {
                Some(values) => Ok(values[row].clone()),
                None => key.expression.evaluate(batch, row),
            }
        }).collect::<Result<Vec<_>>>()?;
        let candidate = Candidate { row, values, options: Arc::clone(&options) };
        if heap.len() < k {
            heap.push(candidate);
        } else if let Some(worst) = heap.peek() {
            if candidate < *worst {
                *heap.peek_mut().expect("nonempty top-K heap") = candidate;
            }
        }
    }
    let mut retained = heap.into_vec();
    retained.sort();
    check()?;
    batch.take(&retained.into_iter().map(|row| row.row).collect::<Vec<_>>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{expr::Expr, scalar::Value, schema::{DataType, Field, Schema}, sort};

    #[test]
    fn zero_limit_still_validates_ordering_expressions() {
        let batch = Batch::empty(Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap());
        let missing = [SortKey::ascending(Expr::Column("unknown".into()))];
        assert!(top_k(&batch, &missing, 0).is_err());
        assert!(top_k(&batch, &missing, 5).is_err());
    }

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
    top_k_offset_checked(batch, keys, offset, count, || Ok(()))
}

pub fn top_k_offset_checked(
    batch: &Batch, keys: &[SortKey], offset: usize, count: usize,
    mut check: impl FnMut() -> Result<()>,
) -> Result<Batch> {
    check()?;
    if count == 0 {
        for key in keys {
            key.expression.output_type(batch.schema())?;
        }
        return batch.take(&[]);
    }
    let required = offset.checked_add(count)
        .ok_or_else(|| crate::Error::Execution("top-K pagination overflow".into()))?;
    let best = top_k_checked(batch, keys, required, || check())?;
    check()?;
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


#[cfg(test)]
mod unordered_topk_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn unordered_topk_keeps_input_prefix() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("value", DataType::Integer, false)]).unwrap(),
            (0..6).map(|n| vec![Value::Integer(n)]).collect()
        ).unwrap();
        assert_eq!(top_k(&batch, &[], 2).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(0)], vec![Value::Integer(1)]]);
        assert_eq!(top_k(&batch, &[], 100).unwrap(), batch);
        assert_eq!(top_k(&batch, &[], 0).unwrap().num_rows(), 0);
    }
}

#[cfg(test)]
mod consolidated_topk_fastpath_tests {
    use super::*;
    use crate::{expr::Expr, schema::{DataType, Field, Schema}};
    #[test]
    fn full_topk_uses_stable_sort_equivalence_and_zero_count_validates() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema, vec![vec![Value::Integer(2)], vec![Value::Integer(1)], vec![Value::Integer(2)]]).unwrap();
        let key = SortKey::ascending(Expr::Column("n".into()));
        assert_eq!(top_k(&batch, &[key.clone()], 4).unwrap(), crate::sort::sort(&batch, &[key.clone()]).unwrap());
        assert!(top_k_offset(&batch, &[SortKey::ascending(Expr::Column("missing".into()))], 0, 0).is_err());
        assert_eq!(top_k_offset(&batch, &[key], 1, 0).unwrap().num_rows(), 0);
    }
}

#[cfg(test)]
mod column_topk_perf_tests {
    use super::*;
    use crate::{expr::{BinaryOp, Expr}, scalar::Value, schema::{DataType, Field, Schema}, sort::SortKey};

    #[test]
    fn borrowed_topk_matches_computed_sort_with_long_text_keys() {
        let schema = Schema::new(vec![Field::new("key", DataType::Text, true)]).unwrap();
        let data = (0..30).map(|n| vec![if n % 7 == 0 { Value::Null }
            else { Value::Text(format!("prefix-{n:04}-{}", "x".repeat(100))) }]).collect();
        let batch = Batch::from_rows(schema, data).unwrap();
        let keys = [SortKey::ascending(Expr::Column("key".into()))];
        let ordered = crate::sort::sort(&batch, &keys).unwrap();
        for k in [1, 3, 15, 29] {
            assert_eq!(top_k(&batch, &keys, k).unwrap(), ordered.slice_rows(0, k).unwrap());
        }
        let numbers = Batch::from_rows(Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(3)], vec![Value::Integer(1)], vec![Value::Integer(2)]]).unwrap();
        let computed = [SortKey::ascending(Expr::binary(Expr::Column("n".into()), BinaryOp::Add, Expr::Literal(Value::Integer(1))))];
        assert_eq!(top_k(&numbers, &computed, 2).unwrap(),
            crate::sort::sort(&numbers, &computed).unwrap().slice_rows(0, 2).unwrap());
    }
}

#[cfg(test)]
mod column_topk_heap_root_tests {
    use super::*;
    use crate::{expr::Expr, scalar::Value, schema::{DataType, Field, Schema}};
    #[test]
    fn column_heap_replacement_matches_full_stable_sort_with_many_updates() {
        let schema = Schema::new(vec![
            Field::new("score", DataType::Integer, true),
            Field::new("id", DataType::Integer, false),
        ]).unwrap();
        let rows = (0..2000).map(|id| vec![
            if id % 13 == 0 { Value::Null }
            else { Value::Integer(((id * 113) % 101) as i64) },
            Value::Integer(id as i64),
        ]).collect();
        let batch = Batch::from_rows(schema, rows).unwrap();
        let keys = [SortKey { expression: Expr::Column("score".into()),
            descending: true, nulls_first: false }];
        let sorted = crate::sort::sort(&batch, &keys).unwrap();
        for count in [1, 3, 32, 513, 1999] {
            assert_eq!(top_k(&batch, &keys, count).unwrap(),
                sorted.slice_rows(0, count).unwrap());
        }
    }
}

#[cfg(test)]
mod singleton_topk_column_scan_tests {
    use super::*;
    use crate::{expr::Expr, scalar::Value, schema::{DataType, Field, Schema}, sort};

    #[test]
    fn one_row_topk_preserves_null_nan_descending_and_stable_ties() {
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("score", DataType::Float, true),
            Field::new("id", DataType::Integer, false),
        ]).unwrap(), vec![
            vec![Value::Float(3.0), Value::Integer(1)],
            vec![Value::Null, Value::Integer(2)],
            vec![Value::Float(1.0), Value::Integer(3)],
            vec![Value::Float(1.0), Value::Integer(4)],
            vec![Value::Float(f64::NAN), Value::Integer(5)],
        ]).unwrap();
        for (descending, nulls_first) in [(false, false), (false, true),
            (true, false), (true, true)] {
            let keys = [SortKey {
                expression: Expr::Column("score".into()), descending, nulls_first,
            }];
            let expected = sort::sort(&batch, &keys).unwrap();
            let result = top_k(&batch, &keys, 1).unwrap();
            assert_eq!(result.num_rows(), 1);
            assert_eq!(result.value(0, 1).unwrap(), expected.value(0, 1).unwrap());
        }
    }
}

#[cfg(test)]
mod native_topk_cancel_during_scan_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn single_key_topk_observes_cancellation_before_completing_scan() {
        let schema = Schema::new(vec![Field::new("score", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema, (0..5000)
            .map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let keys = [SortKey::ascending(Expr::Column("score".into()))];
        let mut checks = 0;
        let error = top_k_offset_checked(&batch, &keys, 0, 1, || {
            checks += 1;
            if checks == 3 { Err(crate::Error::Execution("topk cancelled".into())) }
            else { Ok(()) }
        }).unwrap_err();
        assert!(error.to_string().contains("cancelled"));
        assert_eq!(checks, 3);
        let normal = top_k_offset(&batch, &keys, 0, 1).unwrap();
        assert_eq!(normal.row(0).unwrap(), vec![Value::Integer(0)]);
    }
}

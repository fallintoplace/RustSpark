//! Top-K selection using O(k) candidate memory instead of a full sort.
use std::{borrow::Cow, cmp::Ordering, collections::BinaryHeap, sync::Arc};
use crate::{batch::Batch, expr::Expr, scalar::Value, sort::SortKey, Result};

#[derive(Clone, Debug)]
struct Candidate<'a> {
    row: usize,
    values: Vec<Cow<'a, Value>>,
    options: Arc<Vec<(bool, bool)>>,
}

fn compare(left: &Candidate<'_>, right: &Candidate<'_>) -> Ordering {
    for ((a, b), (descending, nulls_first)) in left.values.iter()
        .zip(&right.values).zip(left.options.iter()) {
        let order = crate::sort::compare_values(a, b, *descending, *nulls_first);
        if order != Ordering::Equal { return order; }
    }
    left.row.cmp(&right.row)
}

impl PartialEq for Candidate<'_> {
    fn eq(&self, other: &Self) -> bool { compare(self, other) == Ordering::Equal }
}
impl Eq for Candidate<'_> {}
impl PartialOrd for Candidate<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
impl Ord for Candidate<'_> {
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

// A common ORDER BY expression has only one computed key. It does not
// need a Vec<Cow<Value>> and an Arc clone for every candidate row.
#[derive(Debug)]
struct ExpressionCandidate {
    row: usize,
    value: Value,
    descending: bool,
    nulls_first: bool,
}
impl PartialEq for ExpressionCandidate {
    fn eq(&self, other: &Self) -> bool { self.cmp(other) == Ordering::Equal }
}
impl Eq for ExpressionCandidate {}
impl PartialOrd for ExpressionCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
impl Ord for ExpressionCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        crate::sort::compare_values(
            &self.value, &other.value, self.descending, self.nulls_first,
        ).then_with(|| self.row.cmp(&other.row))
    }
}

fn top_k_expression(batch: &Batch, key: &SortKey, k: usize) -> Result<Batch> {
    let mut heap = BinaryHeap::with_capacity(k.min(batch.num_rows()));
    for row in 0..batch.num_rows() {
        let candidate = ExpressionCandidate {
            row, value: key.expression.evaluate(batch, row)?,
            descending: key.descending, nulls_first: key.nulls_first,
        };
        if heap.len() < k {
            heap.push(candidate);
        } else if let Some(mut worst) = heap.peek_mut() {
            if candidate < *worst { *worst = candidate; }
        }
    }
    let mut retained = heap.into_vec();
    retained.sort_unstable();
    batch.take(&retained.into_iter().map(|candidate| candidate.row).collect::<Vec<_>>())
}

fn top_k_columns(batch: &Batch, keys: &[SortKey], k: usize) -> Result<Batch> {
    let columns = keys.iter().map(|key| match &key.expression {
        crate::expr::Expr::Column(name) => batch.column(batch.schema().column(name)?),
        _ => unreachable!("column-only Top-K"),
    }).collect::<Result<Vec<_>>>()?;
    let options = keys.iter().map(|key| (key.descending, key.nulls_first)).collect::<Vec<_>>();
    let mut heap = BinaryHeap::with_capacity(k);
    for row in 0..batch.num_rows() {
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

// Retain only row IDs so owned execution can move large payloads instead
// of cloning selected TEXT cells after a mixed-expression heap selection.
fn mixed_top_k_indices(batch: &Batch, keys: &[SortKey], k: usize) -> Result<Vec<usize>> {
    let options = Arc::new(keys.iter().map(|k| (k.descending, k.nulls_first)).collect());
    let key_columns = keys.iter().map(|key| match &key.expression {
        Expr::Column(name) => {
            let index = batch.schema().column(name)?;
            Ok(Some(batch.column(index)?))
        }
        _ => Ok(None),
    }).collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::with_capacity(k.min(batch.num_rows()));
    // Only heap members need a key vector and shared options. Reuse the
    // rejected candidate buffer instead of allocating once per input row.
    let mut scratch = Vec::with_capacity(keys.len());
    for row in 0..batch.num_rows() {
        scratch.clear();
        for (key, column) in keys.iter().zip(&key_columns) {
            scratch.push(match column {
                Some(values) => Cow::Borrowed(&values[row]),
                None => Cow::Owned(key.expression.evaluate(batch, row)?),
            });
        }
        if heap.len() < k {
            heap.push(Candidate {
                row,
                values: std::mem::replace(&mut scratch, Vec::with_capacity(keys.len())),
                options: Arc::clone(&options),
            });
        } else if let Some(mut worst) = heap.peek_mut() {
            let order = scratch.iter().zip(&worst.values).zip(options.iter())
                .map(|((current, retained), (descending, nulls_first))| {
                    crate::sort::compare_values(current, retained, *descending, *nulls_first)
                })
                .find(|order| *order != Ordering::Equal)
                .unwrap_or(Ordering::Equal);
            // Later rows lose stable ties. Swapping key storage reuses the
            // displaced worst candidate on the next input row.
            if order == Ordering::Less {
                std::mem::swap(&mut scratch, &mut worst.values);
                worst.row = row;
            }
        }
    }
    let mut retained = heap.into_vec();
    retained.sort();
    Ok(retained.into_iter().map(|row| row.row).collect())
}

pub fn top_k(batch: &Batch, keys: &[SortKey], k: usize) -> Result<Batch> {
    for key in keys {
        key.expression.output_type(batch.schema())?;
    }
    if k == 0 { return batch.take(&[]); }
    if k >= batch.num_rows() && !keys.is_empty() {
        return crate::sort::sort(batch, keys);
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
                if crate::sort::compare_values(
                    &column[row], &column[best], keys[0].descending, keys[0].nulls_first,
                ) == Ordering::Less {
                    best = row;
                }
            }
            return batch.take(&[best]);
        }
        // A single computed sort key also needs only the best candidate.
        // Evaluate every row to preserve expression error semantics.
        let key = &keys[0];
        let mut best_row = 0usize;
        let mut best_value = key.expression.evaluate(batch, 0)?;
        for row in 1..batch.num_rows() {
            let value = key.expression.evaluate(batch, row)?;
            if crate::sort::compare_values(
                &value, &best_value, key.descending, key.nulls_first,
            ) == Ordering::Less {
                best_row = row;
                best_value = value;
            }
        }
        return batch.take(&[best_row]);
    }
    if k == 1 && keys.len() > 1 {
        // No heap is needed to retain a single best row. Reuse the candidate
        // buffers and borrow direct keys while evaluating every computed key.
        let bound = keys.iter().map(|key| match &key.expression {
            Expr::Column(name) => Ok(Some(batch.column(batch.schema().column(name)?)?)),
            _ => Ok(None),
        }).collect::<Result<Vec<_>>>()?;
        let mut best_values: Vec<Cow<'_, Value>> = Vec::with_capacity(keys.len());
        let mut current_values: Vec<Cow<'_, Value>> = Vec::with_capacity(keys.len());
        let mut best_row = 0usize;
        for row in 0..batch.num_rows() {
            current_values.clear();
            for (key, column) in keys.iter().zip(&bound) {
                current_values.push(match column {
                    Some(values) => Cow::Borrowed(&values[row]),
                    None => Cow::Owned(key.expression.evaluate(batch, row)?),
                });
            }
            let mut order = Ordering::Equal;
            if row != 0 {
                for ((current, best), key) in current_values.iter()
                    .zip(&best_values).zip(keys) {
                    order = crate::sort::compare_values(
                        current, best, key.descending, key.nulls_first);
                    if order != Ordering::Equal { break; }
                }
            }
            if row == 0 || order == Ordering::Less {
                std::mem::swap(&mut best_values, &mut current_values);
                best_row = row;
            }
        }
        return batch.take(&[best_row]);
    }
    if keys.iter().all(|key| matches!(&key.expression, crate::expr::Expr::Column(_))) {
        return top_k_columns(batch, keys, k);
    }
    if keys.len() == 1 {
        return top_k_expression(batch, &keys[0], k);
    }
    batch.take(&mixed_top_k_indices(batch, keys, k)?)
}

/// Top-K for owned input batches. Direct-column and single-expression
/// keys keep only O(k) candidates and transfer retained row payloads without
/// deep-copying TEXT. Other key shapes retain the borrowed implementation.
pub fn top_k_owned(batch: Batch, keys: &[SortKey], k: usize) -> Result<Batch> {
    for key in keys {
        key.expression.output_type(batch.schema())?;
    }
    if k == 0 { return batch.take_unique_owned(&[]); }
    if keys.is_empty() {
        return batch.slice_rows_owned(0, k.min(batch.num_rows()));
    }
    if k >= batch.num_rows() { return crate::sort::sort_owned(batch, keys); }

    if keys.iter().all(|key| matches!(&key.expression, Expr::Column(_))) {
        let columns = keys.iter().map(|key| match &key.expression {
            Expr::Column(name) => batch.column(batch.schema().column(name)?),
            _ => unreachable!("direct column keys"),
        }).collect::<Result<Vec<_>>>()?;
        let options = keys.iter().map(|key| (key.descending, key.nulls_first))
            .collect::<Vec<_>>();
        let mut heap = BinaryHeap::with_capacity(k);
        for row in 0..batch.num_rows() {
            let candidate = ColumnCandidate { row, columns: &columns, options: &options };
            if heap.len() < k {
                heap.push(candidate);
            } else if let Some(mut worst) = heap.peek_mut() {
                if candidate < *worst { *worst = candidate; }
            }
        }
        let mut selected = heap.into_vec();
        selected.sort_unstable();
        let indices = selected.into_iter().map(|candidate| candidate.row)
            .collect::<Vec<_>>();
        return batch.take_unique_owned(&indices);
    }
    if keys.len() == 1 {
        let key = &keys[0];
        let mut heap = BinaryHeap::with_capacity(k);
        for row in 0..batch.num_rows() {
            let candidate = ExpressionCandidate {
                row, value: key.expression.evaluate(&batch, row)?,
                descending: key.descending, nulls_first: key.nulls_first,
            };
            if heap.len() < k {
                heap.push(candidate);
            } else if let Some(mut worst) = heap.peek_mut() {
                if candidate < *worst { *worst = candidate; }
            }
        }
        let mut selected = heap.into_vec();
        selected.sort_unstable();
        let indices = selected.into_iter().map(|candidate| candidate.row)
            .collect::<Vec<_>>();
        return batch.take_unique_owned(&indices);
    }
    let indices = mixed_top_k_indices(&batch, keys, k)?;
    batch.take_unique_owned(&indices)
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
    if count == 0 {
        for key in keys {
            key.expression.output_type(batch.schema())?;
        }
        return batch.take(&[]);
    }
    let required = offset.checked_add(count)
        .ok_or_else(|| crate::Error::Execution("top-K pagination overflow".into()))?;
    // No row can survive an offset past the end. Column-only sort keys
    // cannot fail during evaluation, so avoid a full sort and row copies.
    if offset >= batch.num_rows() &&
        keys.iter().all(|key| matches!(&key.expression, Expr::Column(_))) {
        for key in keys {
            key.expression.output_type(batch.schema())?;
        }
        return batch.take(&[]);
    }
    let best = top_k(batch, keys, required)?;
    // The candidate batch is owned; keep its text allocations when slicing.
    crate::limit::limit_owned(best, offset, Some(count))
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
mod topk_out_of_range_offset_shortcut_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn offset_past_rows_keeps_schema_and_validates_sort_keys() {
        let schema = Schema::new(vec![
            Field::new("text", DataType::Text, true),
            Field::new("id", DataType::Integer, false),
        ]).unwrap();
        let batch = Batch::from_rows(schema.clone(), (0..128).rev()
            .map(|id| vec![
                Value::Text(format!("{}-{id}", "long".repeat(100))),
                Value::Integer(id),
            ]).collect()).unwrap();
        let key = SortKey::ascending(Expr::Column("text".into()));
        for offset in [128, 129, usize::MAX - 2] {
            let actual = top_k_offset(&batch, &[key.clone()], offset, 1).unwrap();
            assert_eq!(actual.num_rows(), 0);
            assert_eq!(actual.schema(), &schema);
        }
        let invalid = SortKey::ascending(Expr::Column("missing".into()));
        assert!(top_k_offset(&batch, &[invalid], 128, 1).is_err());
        assert!(top_k_offset(&batch, &[key], usize::MAX, 1).is_err());
    }
}

#[cfg(test)]
mod topk_borrowed_mixed_expression_keys {
    use super::*;
    use crate::{expr::{BinaryOp, Expr}, schema::{DataType, Field, Schema}};

    #[test]
    fn computed_and_borrowed_keys_match_stable_sort_for_large_payloads() {
        let schema = Schema::new(vec![
            Field::new("label", DataType::Text, true),
            Field::new("number", DataType::Integer, false),
        ]).unwrap();
        let rows = (0..330).map(|index| vec![
            if index % 19 == 0 { Value::Null }
            else { Value::Text(format!("{}-{:-03}", "long-key".repeat(100), index % 23)) },
            Value::Integer((index * 31 % 47) as i64),
        ]).collect();
        let batch = Batch::from_rows(schema, rows).unwrap();
        let keys = [
            SortKey { expression: Expr::Column("label".into()),
                descending: true, nulls_first: false },
            SortKey::ascending(Expr::binary(Expr::Column("number".into()),
                BinaryOp::Add, Expr::Literal(Value::Integer(1)))),
        ];
        let sorted = crate::sort::sort(&batch, &keys).unwrap();
        for size in [0, 1, 3, 29, 127, 329, 330, 400] {
            assert_eq!(top_k(&batch, &keys, size).unwrap(),
                sorted.slice_rows(0, size.min(sorted.num_rows())).unwrap());
        }
    }

    #[test]
    fn mixed_keys_still_evaluate_computed_expressions_on_discarded_rows() {
        let schema = Schema::new(vec![
            Field::new("label", DataType::Text, false),
            Field::new("n", DataType::Integer, false),
        ]).unwrap();
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Text("b".into()), Value::Integer(1)],
            vec![Value::Text("z".into()), Value::Integer(i64::MAX)],
        ]).unwrap();
        let keys = [
            SortKey::ascending(Expr::Column("label".into())),
            SortKey::ascending(Expr::binary(Expr::Column("n".into()),
                BinaryOp::Add, Expr::Literal(Value::Integer(1)))),
        ];
        assert!(top_k(&batch, &keys, 1).is_err());
    }
}

#[cfg(test)]
mod computed_single_key_heap_tests {
    use super::*;
    use crate::{expr::{BinaryOp, Expr}, schema::{DataType, Field, Schema}};

    #[test]
    fn computed_key_heap_matches_stable_full_sort_for_nulls_and_ties() {
        let schema = Schema::new(vec![
            Field::new("n", DataType::Integer, true),
            Field::new("label", DataType::Text, false),
        ]).unwrap();
        let input = Batch::from_rows(schema, (0..200).map(|i| vec![
            if i % 17 == 0 { Value::Null } else { Value::Integer((i % 11) as i64) },
            Value::Text(format!("{i}-{}", "x".repeat(128))),
        ]).collect()).unwrap();
        let expression = Expr::binary(
            Expr::Column("n".into()), BinaryOp::Add, Expr::Literal(Value::Integer(2)),
        );
        for descending in [false, true] {
            for nulls_first in [false, true] {
                let keys = [SortKey { expression: expression.clone(), descending, nulls_first }];
                let expected = crate::sort::sort(&input, &keys).unwrap();
                for k in [1, 2, 17, 101, 199] {
                    assert_eq!(top_k(&input, &keys, k).unwrap(),
                        expected.slice_rows(0, k).unwrap());
                }
            }
        }
    }
}

#[cfg(test)]
mod topk_computed_singleton_and_owned_offset_regressions {
    use super::*;
    use crate::{expr::{BinaryOp, Expr}, schema::{DataType, Field, Schema}};

    #[test]
    fn computed_limit_one_matches_stable_sort_for_ties_and_nulls() {
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("n", DataType::Float, true),
            Field::new("payload", DataType::Text, false),
        ]).unwrap(), vec![
            vec![Value::Float(3.0), Value::Text("first".into())],
            vec![Value::Null, Value::Text("null".into())],
            vec![Value::Float(1.0), Value::Text("winner".into())],
            vec![Value::Float(1.0), Value::Text("tie".into())],
            vec![Value::Float(-1.0), Value::Text("negative".into())],
        ]).unwrap();
        for (descending, nulls_first) in [
            (false, false), (false, true), (true, false), (true, true)
        ] {
            let key = SortKey {
                expression: Expr::binary(Expr::Column("n".into()),
                    BinaryOp::Add, Expr::Literal(Value::Float(0.0))),
                descending, nulls_first,
            };
            let expected = crate::sort::sort(&batch, &[key.clone()]).unwrap();
            assert_eq!(top_k(&batch, &[key], 1).unwrap().rows().unwrap(),
                expected.slice_rows(0, 1).unwrap().rows().unwrap());
        }
    }

    #[test]
    fn computed_limit_one_still_evaluates_later_failing_rows() {
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("n", DataType::Integer, false)
        ]).unwrap(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(i64::MAX)],
        ]).unwrap();
        let key = SortKey::ascending(Expr::binary(
            Expr::Column("n".into()), BinaryOp::Add,
            Expr::Literal(Value::Integer(1)),
        ));
        assert!(top_k(&batch, &[key], 1).is_err());
    }

    #[test]
    fn paginated_topk_keeps_stable_order_on_wide_text() {
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("score", DataType::Integer, false),
            Field::new("label", DataType::Text, true),
        ]).unwrap(), (0..120).map(|index| vec![
            Value::Integer((index * 19 % 37) as i64),
            if index % 13 == 0 { Value::Null }
            else { Value::Text(format!("{index}-{}", "🙂".repeat(128))) },
        ]).collect()).unwrap();
        let key = SortKey::ascending(Expr::Column("score".into()));
        let sorted = crate::sort::sort(&batch, &[key.clone()]).unwrap();
        for offset in [0usize, 1, 13, 111, 120, 200] {
            let start = offset.min(sorted.num_rows());
            let end = start.saturating_add(7).min(sorted.num_rows());
            assert_eq!(top_k_offset(&batch, &[key.clone()], offset, 7).unwrap(),
                sorted.slice_rows(start, end).unwrap());
        }
    }
}


#[cfg(test)]
mod owned_topk_window_tests {
    use super::*;
    use crate::{expr::Expr, scalar::Value,
        schema::{DataType, Field, Schema}, sort::SortKey};

    #[test]
    fn offset_window_matches_stable_sort_for_large_nullable_text() {
        let schema = Schema::new(vec![
            Field::new("score", DataType::Integer, true),
            Field::new("text", DataType::Text, true),
        ]).unwrap();
        let input = Batch::from_rows(schema, (0..240).map(|index| vec![
            if index % 7 == 0 { Value::Null } else { Value::Integer((index * 17 % 29) as i64) },
            if index % 11 == 0 { Value::Null }
            else { Value::Text(format!("{index}-{}", "🙂".repeat(300))) },
        ]).collect()).unwrap();
        for (descending, nulls_first) in [
            (false, false), (false, true), (true, false), (true, true),
        ] {
            let keys = [SortKey {
                expression: Expr::Column("score".into()), descending, nulls_first,
            }];
            let sorted = crate::sort::sort(&input, &keys).unwrap();
            for (offset, count) in [(0, 1), (1, 5), (80, 12), (250, 10)] {
                let start = offset.min(sorted.num_rows());
                let end = start.saturating_add(count).min(sorted.num_rows());
                assert_eq!(top_k_offset(&input, &keys, offset, count).unwrap(),
                    sorted.slice_rows(start, end).unwrap());
            }
        }
    }
}

#[cfg(test)]
mod multikey_single_topk_tests {
    use super::*;
    use crate::{expr::BinaryOp, schema::{DataType, Field, Schema}};

    #[test]
    fn mixed_keys_top_one_agrees_with_full_sort_and_stable_ties() {
        let schema = Schema::new(vec![
            Field::new("score", DataType::Integer, false),
            Field::new("label", DataType::Text, true),
        ]).unwrap();
        let input = Batch::from_rows(schema, vec![
            vec![Value::Integer(2), Value::Text("z".repeat(4096))],
            vec![Value::Integer(1), Value::Text("gamma".into())],
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(1), Value::Text("alpha".into())],
            vec![Value::Integer(1), Value::Null],
        ]).unwrap();
        let keys = vec![
            SortKey::ascending(Expr::binary(
                Expr::Column("score".into()), BinaryOp::Add,
                Expr::Literal(Value::Integer(0)))),
            SortKey::ascending(Expr::Column("label".into())),
        ];
        assert_eq!(top_k(&input, &keys, 1).unwrap(),
            crate::sort::sort(&input, &keys).unwrap().slice_rows(0, 1).unwrap());
        assert_eq!(top_k(&input, &keys, 1).unwrap().row(0).unwrap(),
            vec![Value::Integer(1), Value::Null]);
    }

    #[test]
    fn later_computed_key_errors_are_not_hidden_by_an_earlier_key() {
        let schema = Schema::new(vec![
            Field::new("score", DataType::Integer, false),
            Field::new("divisor", DataType::Integer, false),
        ]).unwrap();
        let input = Batch::from_rows(schema, vec![
            vec![Value::Integer(0), Value::Integer(1)],
            vec![Value::Integer(3), Value::Integer(0)],
        ]).unwrap();
        let keys = vec![
            SortKey::ascending(Expr::Column("score".into())),
            SortKey::ascending(Expr::binary(
                Expr::Literal(Value::Integer(10)), BinaryOp::Divide,
                Expr::Column("divisor".into()))),
        ];
        assert!(top_k(&input, &keys, 1).is_err());
    }
}


#[cfg(test)]
mod mixed_topk_key_buffer_regressions {
    use super::*;
    use crate::{expr::BinaryOp, schema::{DataType, Field, Schema}};

    #[test]
    fn mixed_computed_topk_matches_stable_full_sort_after_many_replacements() {
        let schema = Schema::new(vec![
            Field::new("score", DataType::Integer, false),
            Field::new("payload", DataType::Text, false),
        ]).unwrap();
        let batch = Batch::from_rows(schema, (0..512).map(|n| vec![
            Value::Integer((n * 17 % 41) as i64),
            Value::Text(format!("row-{n}-{}", "x".repeat(256))),
        ]).collect()).unwrap();
        let keys = [
            SortKey::ascending(Expr::Column("score".into())),
            SortKey::ascending(Expr::binary(
                Expr::Column("score".into()), BinaryOp::Add,
                Expr::Literal(Value::Integer(1)),
            )),
        ];
        let sorted = crate::sort::sort(&batch, &keys).unwrap();
        for limit in [2, 13, 64, 300] {
            assert_eq!(top_k(&batch, &keys, limit).unwrap(),
                sorted.slice_rows(0, limit).unwrap());
        }
    }
}
#[cfg(test)]
mod owned_topk_payload_regressions {
    use super::*;
    use crate::{expr::BinaryOp, schema::{DataType, Field, Schema}};

    #[test]
    fn direct_and_computed_keys_move_text_and_match_borrowed_topk() {
        let input = Batch::from_rows(Schema::new(vec![
            Field::new("score", DataType::Integer, true),
            Field::new("payload", DataType::Text, false),
        ]).unwrap(), vec![
            vec![Value::Integer(6), Value::Text("a".repeat(100))],
            vec![Value::Integer(1), Value::Text("🙂".repeat(4096))],
            vec![Value::Integer(3), Value::Text("b".repeat(4096))],
            vec![Value::Null, Value::Text("null".into())],
            vec![Value::Integer(1), Value::Text("tie".into())],
        ]).unwrap();
        let ptr = match input.value(1, 1).unwrap() {
            Value::Text(value) => value.as_ptr(), _ => unreachable!(),
        };
        let direct = [SortKey::ascending(Expr::Column("score".into()))];
        let expected = top_k(&input, &direct, 3).unwrap();
        let moved = top_k_owned(input.clone(), &direct, 3).unwrap();
        assert_eq!(moved, expected);
        let moved_original = top_k_owned(input.clone(), &direct, 3).unwrap();
        assert_eq!(moved_original.num_rows(), 3);

        let computed = [SortKey::ascending(Expr::binary(
            Expr::Column("score".into()), BinaryOp::Add,
            Expr::Literal(Value::Integer(1)),
        ))];
        assert_eq!(top_k_owned(input.clone(), &computed, 3).unwrap(),
            top_k(&input, &computed, 3).unwrap());
        let all = top_k_owned(input.clone(), &direct, 100).unwrap();
        assert_eq!(all, top_k(&input, &direct, 100).unwrap());
        let moved_input = top_k_owned(input, &direct, 3).unwrap();
        // ASC puts NULL first; the first score=1 row follows it.
        assert_eq!(moved_input.value(0, 0).unwrap(), &Value::Null);
        assert_eq!(moved_input.value(1, 0).unwrap(), &Value::Integer(1));
        assert_eq!(match moved_input.value(1, 1).unwrap() {
            Value::Text(value) => value.as_ptr(), _ => unreachable!(),
        }, ptr);
    }

    #[test]
    fn empty_sort_keys_keep_zero_width_row_count_and_limits() {
        let schema = Schema::new(vec![]).unwrap();
        let input = Batch::new_with_row_count(schema, vec![], 5).unwrap();
        assert_eq!(top_k_owned(input.clone(), &[], 2).unwrap().num_rows(), 2);
        assert_eq!(top_k_owned(input, &[], 0).unwrap().num_rows(), 0);
    }
}


#[cfg(test)]
mod owned_mixed_multikey_topk_tests {
    use super::*;
    use crate::{expr::BinaryOp, schema::{DataType, Field, Schema}};

    #[test]
    fn moves_selected_text_for_computed_and_direct_sort_keys() {
        let input = Batch::from_rows(Schema::new(vec![
            Field::new("score", DataType::Integer, false),
            Field::new("payload", DataType::Text, false),
        ]).unwrap(), (0..128).map(|i| vec![
            Value::Integer(((i * 17) % 31) as i64),
            Value::Text(format!("{i}-{}", "é".repeat(2048))),
        ]).collect()).unwrap();
        let keys = [
            SortKey::ascending(Expr::binary(
                Expr::Column("score".into()), BinaryOp::Add,
                Expr::Literal(Value::Integer(1)),
            )),
            SortKey::ascending(Expr::Column("payload".into())),
        ];
        let expected = top_k(&input, &keys, 9).unwrap();
        let expected_pointer = match input.value(0, 1).unwrap() {
            Value::Text(text) => text.as_ptr(),
            _ => unreachable!(),
        };
        let expected_index = expected.rows().unwrap().iter().position(
            |row| row[1] == *input.value(0, 1).unwrap(),
        );
        let result = top_k_owned(input, &keys, 9).unwrap();
        assert_eq!(result, expected);
        if let Some(index) = expected_index {
            let actual = match result.value(index, 1).unwrap() {
                Value::Text(text) => text.as_ptr(),
                _ => unreachable!(),
            };
            assert_eq!(actual, expected_pointer);
        }
    }

    #[test]
    fn evaluates_later_computed_keys_even_for_discarded_rows() {
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("score", DataType::Integer, false),
            Field::new("divisor", DataType::Integer, false),
        ]).unwrap(), vec![
            vec![Value::Integer(0), Value::Integer(1)],
            vec![Value::Integer(999), Value::Integer(0)],
            vec![Value::Integer(1000), Value::Integer(1)],
        ]).unwrap();
        let keys = [
            SortKey::ascending(Expr::Column("score".into())),
            SortKey::ascending(Expr::binary(
                Expr::Literal(Value::Integer(10)), BinaryOp::Divide,
                Expr::Column("divisor".into())),
        ];
        assert!(top_k_owned(batch, &keys, 1).is_err());
    }
}

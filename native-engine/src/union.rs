use crate::{batch::Batch, scalar::Value, Error, Result};
#[cfg(test)]
use crate::schema::Schema;

pub fn union_all(left: &Batch, right: &Batch) -> Result<Batch> {
    let (left, right, schema) = crate::set_ops::align_set_inputs(left, right)?;
    let count = left
        .num_rows()
        .checked_add(right.num_rows())
        .ok_or_else(|| Error::Execution("UNION row count overflow".into()))?;
    let mut columns = Vec::with_capacity(schema.len());
    for column in 0..schema.len() {
        let mut values = Vec::with_capacity(count);
        values.extend_from_slice(left.column(column)?);
        values.extend_from_slice(right.column(column)?);
        columns.push(values);
    }
    Batch::new_with_row_count(schema, columns, count)
}

/// Concatenate temporary inputs by consuming their columns. A changed numeric
/// result type still widens INTEGER values to FLOAT before appending.
pub fn union_all_owned(left: Batch, right: Batch) -> Result<Batch> {
    let schema = crate::set_ops::compatible_schema(&left, &right)?;
    let count = left.num_rows().checked_add(right.num_rows())
        .ok_or_else(|| Error::Execution("UNION row count overflow".into()))?;
    let left_types = left.schema().fields().iter().map(|field| field.data_type)
        .collect::<Vec<_>>();
    let right_types = right.schema().fields().iter().map(|field| field.data_type)
        .collect::<Vec<_>>();
    let mut columns = left.into_columns();
    let mut right_columns = right.into_columns();
    for (index, (left_values, right_values)) in columns.iter_mut()
        .zip(right_columns.iter_mut()).enumerate() {
        let target = schema.fields()[index].data_type;
        for (values, source_type) in [
            (left_values.as_mut_slice(), left_types[index]),
            (right_values.as_mut_slice(), right_types[index]),
        ] {
            if source_type == crate::schema::DataType::Integer &&
                target == crate::schema::DataType::Float {
                for value in values {
                    if let crate::scalar::Value::Integer(number) = value {
                        *value = crate::scalar::Value::Float(*number as f64);
                    }
                }
            }
        }
        left_values.append(right_values);
    }
    Batch::new_with_row_count(schema, columns, count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        scalar::Value,
        schema::{DataType, Field},
    };

    #[test]
    fn union_preserves_duplicates_and_nullable_values() {
        let a = Batch::from_rows(
            Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(1)]],
        )
        .unwrap();
        let b = Batch::from_rows(
            Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap(),
            vec![vec![Value::Null], vec![Value::Integer(1)]],
        )
        .unwrap();
        let merged = union_all(&a, &b).unwrap();
        assert_eq!(merged.num_rows(), 3);
        assert_eq!(merged.row(1).unwrap(), vec![Value::Null]);
    }
}

 
/// UNION DISTINCT matches columns by position and removes repeated complete rows.
pub fn union_distinct(left: &Batch, right: &Batch) -> Result<Batch> {
    union_distinct_with_budget(left, right, usize::MAX, usize::MAX)
}

/// Apply distinct-key and byte budgets while preserving original input order.
pub fn union_distinct_with_budget(
    left: &Batch, right: &Batch, max_keys: usize, max_bytes: usize,
) -> Result<Batch> {
    use std::{
        collections::{HashMap, hash_map::Entry},
        hash::{Hash, Hasher},
    };
    // A key references validated, immutable column slices from either input.
    // Store only row coordinates in the set, not cloned variable-width payloads.
    #[derive(Clone, Copy)]
    struct Key<'a> { columns: &'a [&'a [Value]], row: usize }
    impl Hash for Key<'_> {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.columns.len().hash(state);
            for column in self.columns { column[self.row].hash(state); }
        }
    }
    impl PartialEq for Key<'_> {
        fn eq(&self, other: &Self) -> bool {
            self.columns.len() == other.columns.len() &&
                self.columns.iter().zip(other.columns)
                    .all(|(a, b)| a[self.row] == b[other.row])
        }
    }
    impl Eq for Key<'_> {}

    let (left, right, schema) = crate::set_ops::align_set_inputs(left, right)?;
    if schema.is_empty() {
        let rows = usize::from(left.num_rows() != 0 || right.num_rows() != 0);
        if rows > max_keys {
            return Err(Error::Execution("UNION DISTINCT key budget exceeded".into()));
        }
        if rows != 0 && max_bytes < 64 {
            return Err(Error::Execution("UNION DISTINCT byte budget exceeded".into()));
        }
        return Batch::new_with_row_count(schema, vec![], rows);
    }
    let left_columns = (0..schema.len()).map(|index| left.column(index))
        .collect::<Result<Vec<_>>>()?;
    let right_columns = (0..schema.len()).map(|index| right.column(index))
        .collect::<Result<Vec<_>>>()?;
    let sources = [
        (left.num_rows(), left_columns.as_slice()),
        (right.num_rows(), right_columns.as_slice()),
    ];
    let base = 64usize.checked_add(schema.len()
        .checked_mul(std::mem::size_of::<Value>())
        .ok_or_else(|| Error::Execution("UNION DISTINCT byte budget overflow".into()))?)
        .ok_or_else(|| Error::Execution("UNION DISTINCT byte budget overflow".into()))?;
    let mut seen = HashMap::<Key<'_>, ()>::new();
    let mut columns = (0..schema.len()).map(|_| Vec::new()).collect::<Vec<Vec<Value>>>();
    let mut rows = 0usize;
    let mut bytes = 0usize;
    for (row_count, source) in sources {
        for row in 0..row_count {
            let key = Key { columns: source, row };
            let at_limit = seen.len() >= max_keys;
            match seen.entry(key) {
                Entry::Occupied(_) => {}
                Entry::Vacant(entry) => {
                    if at_limit {
                        return Err(Error::Execution("UNION DISTINCT key budget exceeded".into()));
                    }
                    let footprint = source.iter().try_fold(base, |total, values| {
                        let len = match &values[row] {
                            Value::Text(text) => text.len(),
                            _ => 0,
                        };
                        total.checked_add(len).ok_or_else(||
                            Error::Execution("UNION DISTINCT byte budget overflow".into()))
                    })?;
                    bytes = bytes.checked_add(footprint)
                        .filter(|&total| total <= max_bytes)
                        .ok_or_else(|| Error::Execution("UNION DISTINCT byte budget exceeded".into()))?;
                    for (output, values) in columns.iter_mut().zip(source) {
                        output.push(values[row].clone());
                    }
                    rows += 1;
                    entry.insert(());
                }
            }
        }
    }
    Batch::new_with_row_count(schema, columns, rows)
}

#[cfg(test)]
mod union_distinct_feature_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};

    #[test]
    fn distinct_set_union_collapses_null_and_duplicate_rows() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap();
        let a = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Null],
        ]).unwrap();
        let b = Batch::from_rows(schema, vec![
            vec![Value::Integer(1)], vec![Value::Null], vec![Value::Integer(2)],
        ]).unwrap();
        assert_eq!(union_distinct(&a, &b).unwrap().rows().unwrap(), vec![
            vec![Value::Integer(1)], vec![Value::Null], vec![Value::Integer(2)],
        ]);
    }
}


#[cfg(test)]
mod incremental_union_distinct_regression {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};
    #[test]
    fn deduplicates_across_inputs_and_widens_nullability() {
        let left = Batch::from_rows(Schema::new(vec![
            Field::new("left_id", DataType::Integer, false),
        ]).unwrap(), vec![
            vec![Value::Integer(2)], vec![Value::Integer(2)],
        ]).unwrap();
        let right = Batch::from_rows(Schema::new(vec![
            Field::new("right_id", DataType::Integer, true),
        ]).unwrap(), vec![
            vec![Value::Integer(2)], vec![Value::Null],
            vec![Value::Integer(3)], vec![Value::Integer(3)],
        ]).unwrap();
        let output = union_distinct(&left, &right).unwrap();
        assert_eq!(output.rows().unwrap(), vec![
            vec![Value::Integer(2)], vec![Value::Null], vec![Value::Integer(3)],
        ]);
        assert_eq!(output.schema().fields()[0].name, "left_id");
        assert!(output.schema().fields()[0].nullable);
    }
}

#[cfg(test)]
mod zero_column_union_consolidation_tests {
    use super::*;
    #[test]
    fn union_preserves_zero_width_multiset_cardinality() {
        let schema = Schema::new(vec![]).unwrap();
        let a = Batch::new_with_row_count(schema.clone(), vec![], 2).unwrap();
        let b = Batch::new_with_row_count(schema, vec![], 3).unwrap();
        assert_eq!(union_all(&a, &b).unwrap().rows().unwrap(), vec![vec![]; 5]);
        assert_eq!(union_distinct(&a, &b).unwrap().rows().unwrap(), vec![vec![]]);
    }
}

#[cfg(test)]
mod union_distinct_empty_schema_fast_path_tests {
    use super::*;
    #[test]
    fn avoids_enumerating_every_zero_column_row() {
        let schema = Schema::new(vec![]).unwrap();
        let left = Batch::new_with_row_count(schema.clone(), vec![], 100_000).unwrap();
        let right = Batch::new_with_row_count(schema.clone(), vec![], 200_000).unwrap();
        assert_eq!(union_distinct(&left, &right).unwrap().num_rows(), 1);
        let empty = Batch::new_with_row_count(schema, vec![], 0).unwrap();
        assert_eq!(union_distinct(&empty, &empty).unwrap().num_rows(), 0);
        assert_eq!(union_distinct(&empty, &right).unwrap().num_rows(), 1);
    }
}

#[cfg(test)]
mod borrowed_union_distinct_budget_regressions {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn deduplicates_across_sources_with_long_text_nulls_and_nan() {
        let left_schema = Schema::new(vec![
            Field::new("key", DataType::Text, true),
            Field::new("n", DataType::Float, true),
        ]).unwrap();
        let right_schema = Schema::new(vec![
            Field::new("other", DataType::Text, true),
            Field::new("n", DataType::Float, true),
        ]).unwrap();
        let wide = "🙂".repeat(6000);
        let left = Batch::from_rows(left_schema, vec![
            vec![Value::Text(wide.clone()), Value::Float(-0.0)],
            vec![Value::Null, Value::Float(f64::NAN)],
            vec![Value::Text(wide.clone()), Value::Float(-0.0)],
        ]).unwrap();
        let right = Batch::from_rows(right_schema, vec![
            vec![Value::Text(wide.clone()), Value::Float(0.0)],
            vec![Value::Null, Value::Float(f64::NAN)],
            vec![Value::Text("new".into()), Value::Null],
        ]).unwrap();
        let base = 64 + 2 * std::mem::size_of::<Value>();
        let bytes = base * 3 + wide.len() + 3;
        let result = union_distinct_with_budget(&left, &right, 3, bytes).unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.schema().fields()[0].name, "key");
        assert_eq!(result.value(0, 0).unwrap(), &Value::Text(wide));
        assert_eq!(result.value(1, 0).unwrap(), &Value::Null);
        assert_eq!(result.value(2, 0).unwrap(), &Value::Text("new".into()));
        assert_eq!(result, union_distinct(&left, &right).unwrap());
        assert!(union_distinct_with_budget(&left, &right, 2, bytes).is_err());
        assert!(union_distinct_with_budget(&left, &right, 3, bytes - 1).is_err());
    }

    #[test]
    fn zero_width_key_requires_only_one_admitted_distinct_row() {
        let schema = Schema::new(vec![]).unwrap();
        let left = Batch::new_with_row_count(schema.clone(), vec![], 1_000).unwrap();
        let right = Batch::new_with_row_count(schema.clone(), vec![], 1_000).unwrap();
        assert_eq!(union_distinct_with_budget(&left, &right, 1, 64).unwrap().num_rows(), 1);
        assert!(union_distinct_with_budget(&left, &right, 0, 64).is_err());
        assert!(union_distinct_with_budget(&left, &right, 1, 63).is_err());
        assert_eq!(union_distinct_with_budget(&Batch::empty(schema.clone()),
            &Batch::empty(schema), 0, 0).unwrap().num_rows(), 0);
    }
}

#[cfg(test)]
mod owned_union_all_regressions {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn reuses_wide_text_allocations_from_both_inputs() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Text, true),
            Field::new("n", DataType::Integer, false),
        ]).unwrap();
        let large = "é🙂".repeat(4096);
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text(large.clone()), Value::Integer(1)],
            vec![Value::Null, Value::Integer(2)],
        ]).unwrap();
        let right = Batch::from_rows(schema, vec![
            vec![Value::Text(large.clone()), Value::Integer(3)],
        ]).unwrap();
        let left_ptr = match left.value(0, 0).unwrap() {
            Value::Text(s) => s.as_ptr(), _ => unreachable!(),
        };
        let right_ptr = match right.value(0, 0).unwrap() {
            Value::Text(s) => s.as_ptr(), _ => unreachable!(),
        };
        let expected = union_all(&left, &right).unwrap();
        let result = union_all_owned(left, right).unwrap();
        assert_eq!(result, expected);
        assert_eq!(match result.value(0, 0).unwrap() {
            Value::Text(s) => s.as_ptr(), _ => unreachable!(),
        }, left_ptr);
        assert_eq!(match result.value(2, 0).unwrap() {
            Value::Text(s) => s.as_ptr(), _ => unreachable!(),
        }, right_ptr);
    }

    #[test]
    fn widens_numeric_columns_preserving_order_nullability_and_empty_width() {
        let left = Batch::from_rows(Schema::new(vec![
            Field::new("lhs", DataType::Integer, false),
        ]).unwrap(), vec![
            vec![Value::Integer(7)], vec![Value::Integer(9)],
        ]).unwrap();
        let right = Batch::from_rows(Schema::new(vec![
            Field::new("rhs", DataType::Float, true),
        ]).unwrap(), vec![
            vec![Value::Float(2.5)], vec![Value::Null],
        ]).unwrap();
        let expected = union_all(&left, &right).unwrap();
        let result = union_all_owned(left, right).unwrap();
        assert_eq!(result, expected);
        assert_eq!(result.rows().unwrap(), vec![
            vec![Value::Float(7.0)], vec![Value::Float(9.0)],
            vec![Value::Float(2.5)], vec![Value::Null],
        ]);
        assert_eq!(result.schema().field(0).unwrap().name, "lhs");
        assert!(result.schema().field(0).unwrap().nullable);
        let empty = Schema::new(vec![]).unwrap();
        let one = Batch::new_with_row_count(empty.clone(), vec![], 2).unwrap();
        let two = Batch::new_with_row_count(empty, vec![], 3).unwrap();
        assert_eq!(union_all_owned(one, two).unwrap().num_rows(), 5);
    }
}

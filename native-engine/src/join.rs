use std::collections::{HashMap, HashSet};

use crate::{
    batch::Batch,
    scalar::Value,
    schema::{DataType, Field, Schema},
    Error,
    Result,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
}

const MAX_JOIN_ROWS: usize = 1_000_000;

fn joinable(key: &Value) -> bool {
    !key.is_null() && !matches!(key, Value::Float(value) if value.is_nan())
}

fn normalize_key(value: &Value, numeric_widen: bool) -> Result<Value> {
    if numeric_widen && matches!(value, Value::Integer(_)) {
        value.cast_to(DataType::Float)
    } else {
        Ok(value.clone())
    }
}

pub fn hash_join(
    left: &Batch, right: &Batch, left_key: &str, right_key: &str, kind: JoinKind,
) -> Result<Batch> {
    hash_join_keys(left, right, &[(left_key, right_key)], kind)
}

/// Hash join over conjunctive equality keys. NULL in any key prevents a match,
/// while integer/float pairs are compared in a common numeric type.
pub fn hash_join_keys(
    left: &Batch, right: &Batch, keys: &[(&str, &str)], kind: JoinKind,
) -> Result<Batch> {
    if keys.is_empty() || keys.len() > 16 {
        return Err(Error::Schema("JOIN requires 1 to 16 equality keys".into()));
    }
    let mut columns = Vec::with_capacity(keys.len());
    for &(left_key, right_key) in keys {
        let l = left.schema().column(left_key)?;
        let r = right.schema().column(right_key)?;
        let lt = left.schema().field(l)?.data_type;
        let rt = right.schema().field(r)?.data_type;
        let widen = match (lt, rt) {
            (a, b) if a == b => false,
            (DataType::Integer, DataType::Float)
            | (DataType::Float, DataType::Integer) => true,
            _ => return Err(Error::Type("join key types must match or be numeric".into())),
        };
        columns.push((l, r, widen));
    }
    let row_key = |batch: &Batch, row: usize, left_side: bool| -> Result<Option<Vec<Value>>> {
        let mut key = Vec::with_capacity(columns.len());
        for &(l, r, widen) in &columns {
            let column = if left_side { l } else { r };
            let value = normalize_key(batch.value(row, column)?, widen)?;
            if !joinable(&value) { return Ok(None); }
            key.push(value);
        }
        Ok(Some(key))
    };
    let mut right_hash: HashMap<Vec<Value>, Vec<usize>> = HashMap::new();
    for row in 0..right.num_rows() {
        if let Some(key) = row_key(right, row, false)? {
            right_hash.entry(key).or_default().push(row);
        }
    }
    if matches!(kind, JoinKind::Semi | JoinKind::Anti) {
        let mut selected = Vec::new();
        for row in 0..left.num_rows() {
            let found = row_key(left, row, true)?.is_some_and(|key| right_hash.contains_key(&key));
            if (kind == JoinKind::Semi && found) || (kind == JoinKind::Anti && !found) {
                if selected.len() >= MAX_JOIN_ROWS {
                    return Err(Error::Execution("join output exceeds safety limit".into()));
                }
                selected.push(row);
            }
        }
        return left.take(&selected);
    }
    let mut used: HashSet<String> = left.schema().fields().iter()
        .map(|f| f.name.to_ascii_lowercase()).collect();
    let mut fields = left.schema().fields().iter().map(|field|
        Field::new(&field.name, field.data_type,
            field.nullable || matches!(kind, JoinKind::Right | JoinKind::Full))
    ).collect::<Vec<_>>();
    for field in right.schema().fields() {
        let mut name = field.name.clone();
        while used.contains(&name.to_ascii_lowercase()) {
            name = format!("right_{name}");
        }
        used.insert(name.to_ascii_lowercase());
        fields.push(Field::new(name, field.data_type,
            field.nullable || matches!(kind, JoinKind::Left | JoinKind::Full)));
    }
    let schema = Schema::new(fields)?;
    let mut output = Vec::new();
    let mut matched_right = vec![false; right.num_rows()];
    for left_row in 0..left.num_rows() {
        let key = row_key(left, left_row, true)?;
        let matches = key.as_ref().and_then(|key| right_hash.get(key));
        if let Some(matches) = matches {
            for &right_row in matches {
                matched_right[right_row] = true;
                if output.len() >= MAX_JOIN_ROWS {
                    return Err(Error::Execution("join output exceeds safety limit".into()));
                }
                let mut row = left.row(left_row)?;
                row.extend(right.row(right_row)?);
                output.push(row);
            }
        } else if matches!(kind, JoinKind::Left | JoinKind::Full) {
            if output.len() >= MAX_JOIN_ROWS {
                return Err(Error::Execution("join output exceeds safety limit".into()));
            }
            let mut row = left.row(left_row)?;
            row.extend(vec![Value::Null; right.schema().len()]);
            output.push(row);
        }
    }
    if matches!(kind, JoinKind::Right | JoinKind::Full) {
        for row in 0..right.num_rows() {
            if !matched_right[row] {
                if output.len() >= MAX_JOIN_ROWS {
                    return Err(Error::Execution("join output exceeds safety limit".into()));
                }
                let mut values = vec![Value::Null; left.schema().len()];
                values.extend(right.row(row)?);
                output.push(values);
            }
        }
    }
    Batch::from_rows(schema, output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, Field};

    fn table(values: &[Value], name: &str) -> Batch {
        Batch::from_rows(
            Schema::new(vec![Field::new(name, DataType::Integer, true)]).unwrap(),
            values.iter().cloned().map(|value| vec![value]).collect(),
        )
        .unwrap()
    }

    #[test]
    fn full_outer_join_preserves_both_unmatched_sides() {
        let left = table(&[Value::Integer(1), Value::Integer(2), Value::Null], "id");
        let right = table(&[Value::Integer(2), Value::Integer(3), Value::Null], "id");
        let result = hash_join(&left, &right, "id", "id", JoinKind::Full).unwrap();
        assert_eq!(result.rows().unwrap(), vec![
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Integer(2)],
            vec![Value::Null, Value::Null],
            vec![Value::Null, Value::Integer(3)],
            vec![Value::Null, Value::Null],
        ]);
    }

    #[test]
    fn right_join_includes_unmatched_right_rows_and_null_keys() {
        let left = table(&[Value::Integer(1)], "id");
        let right = table(&[Value::Integer(1), Value::Integer(2), Value::Null], "id");
        let result = hash_join(&left, &right, "id", "id", JoinKind::Right).unwrap();
        assert_eq!(result.rows().unwrap(), vec![
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Null, Value::Integer(2)],
            vec![Value::Null, Value::Null],
        ]);
    }

    #[test]
    fn left_join_preserves_unmatched_rows_and_never_matches_nulls() {
        let left = table(&[Value::Integer(1), Value::Null, Value::Integer(2)], "id");
        let right = table(&[Value::Integer(1), Value::Null], "id");
        let result = hash_join(&left, &right, "id", "id", JoinKind::Left).unwrap();
        assert_eq!(result.rows().unwrap(), vec![
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Null, Value::Null],
            vec![Value::Integer(2), Value::Null],
        ]);
        assert_eq!(result.schema().column("right_id").unwrap(), 1);
    }
    #[test]
    fn empty_inner_join_returns_typed_empty_output() {
        let left = table(&[], "id");
        let right = table(&[Value::Integer(1), Value::Null], "id");
        let result = hash_join(&left, &right, "id", "id", JoinKind::Inner).unwrap();
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.schema().len(), 2);
        assert_eq!(hash_join(&right, &left, "id", "id", JoinKind::Inner).unwrap().num_rows(), 0);
    }
}

#[cfg(test)]
mod nan_join_key_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn nan_keys_do_not_match_in_inner_semi_or_left_joins() {
        let schema = Schema::new(vec![Field::new("n", DataType::Float, true)]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Float(f64::NAN)], vec![Value::Float(1.0)],
        ]).unwrap();
        let right = Batch::from_rows(schema, vec![
            vec![Value::Float(f64::NAN)], vec![Value::Float(1.0)],
        ]).unwrap();
        assert_eq!(hash_join(&left, &right, "n", "n", JoinKind::Inner).unwrap().num_rows(), 1);
        assert_eq!(hash_join(&left, &right, "n", "n", JoinKind::Semi).unwrap().num_rows(), 1);
        assert_eq!(hash_join(&left, &right, "n", "n", JoinKind::Anti).unwrap().num_rows(), 1);
        assert_eq!(hash_join(&left, &right, "n", "n", JoinKind::Left).unwrap().num_rows(), 2);
    }
}

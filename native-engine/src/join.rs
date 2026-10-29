use std::collections::{HashMap, HashSet};

use crate::{
    batch::Batch,
    scalar::Value,
    schema::{Field, Schema},
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


fn typed_join_columns(
    left: &Batch, right: &Batch, keys: &[(String, String)]
) -> Result<(Vec<usize>, Vec<usize>)> {
    if keys.is_empty() {
        return Err(Error::Parse("JOIN requires at least one equality key".into()));
    }
    let mut left_columns = Vec::with_capacity(keys.len());
    let mut right_columns = Vec::with_capacity(keys.len());
    for (left_key, right_key) in keys {
        let left_column = left.schema().column(left_key)?;
        let right_column = right.schema().column(right_key)?;
        if left.schema().field(left_column)?.data_type != right.schema().field(right_column)?.data_type {
            return Err(Error::Type(format!("join key types must match: {left_key} = {right_key}")));
        }
        left_columns.push(left_column);
        right_columns.push(right_column);
    }
    Ok((left_columns, right_columns))
}

fn join_key(batch: &Batch, row: usize, columns: &[usize]) -> Result<Option<Vec<Value>>> {
    let mut key = Vec::with_capacity(columns.len());
    for &column in columns {
        let value = batch.value(row, column)?;
        if !joinable(value) { return Ok(None); }
        key.push(value.clone());
    }
    Ok(Some(key))
}

/// Compatibility wrapper for single-key joins.
pub fn hash_join(
    left: &Batch,
    right: &Batch,
    left_key: &str,
    right_key: &str,
    kind: JoinKind,
) -> Result<Batch> {
    hash_join_multi(left, right, &[(left_key.to_owned(), right_key.to_owned())], kind)
}

/// Hash join with a composite equality key. NULL or NaN in any key component
/// prevents a match, including for LEFT SEMI/ANTI joins.
pub fn hash_join_multi(
    left: &Batch,
    right: &Batch,
    keys: &[(String, String)],
    kind: JoinKind,
) -> Result<Batch> {
    let (left_columns, right_columns) = typed_join_columns(left, right, keys)?;
    if matches!(kind, JoinKind::Semi | JoinKind::Anti) {
        let mut right_keys = HashSet::new();
        for row in 0..right.num_rows() {
            if let Some(key) = join_key(right, row, &right_columns)? {
                right_keys.insert(key);
            }
        }
        let mut indices = Vec::new();
        for row in 0..left.num_rows() {
            let key = join_key(left, row, &left_columns)?;
            let found = key.as_ref().is_some_and(|key| right_keys.contains(key));
            if (kind == JoinKind::Semi && found) || (kind == JoinKind::Anti && !found) {
                if indices.len() >= MAX_JOIN_ROWS {
                    return Err(Error::Execution("join output exceeds safety limit".into()));
                }
                indices.push(row);
            }
        }
        return left.take(&indices);
    }

    let mut used: HashSet<String> = left.schema().fields().iter()
        .map(|field| field.name.to_ascii_lowercase()).collect();
    let mut fields = left.schema().fields().iter().map(|field| {
        Field::new(&field.name, field.data_type,
            field.nullable || matches!(kind, JoinKind::Right | JoinKind::Full))
    }).collect::<Vec<_>>();
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
    if kind == JoinKind::Inner && (left.num_rows() == 0 || right.num_rows() == 0) {
        return Ok(Batch::empty(schema));
    }

    let mut right_hash: HashMap<Vec<Value>, Vec<usize>> = HashMap::new();
    for row in 0..right.num_rows() {
        if let Some(key) = join_key(right, row, &right_columns)? {
            right_hash.entry(key).or_default().push(row);
        }
    }
    let mut output = Vec::new();
    let mut matched_right = vec![false; right.num_rows()];
    for left_row in 0..left.num_rows() {
        let key = join_key(left, left_row, &left_columns)?;
        let matches = key.as_ref().and_then(|key| right_hash.get(key));
        if let Some(right_rows) = matches {
            for &right_row in right_rows {
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
        for right_row in 0..right.num_rows() {
            if !matched_right[right_row] {
                if output.len() >= MAX_JOIN_ROWS {
                    return Err(Error::Execution("join output exceeds safety limit".into()));
                }
                let mut row = vec![Value::Null; left.schema().len()];
                row.extend(right.row(right_row)?);
                output.push(row);
            }
        }
    }
    Batch::from_rows(schema, output)
}

/**
 * Spark JOIN USING merges equality keys into one output column, with the
 * columns listed first in USING order. FULL and RIGHT joins must coalesce
 * keys from unmatched right rows instead of dropping their values.
 */
pub fn using_join(
    left: &Batch,
    right: &Batch,
    keys: &[String],
    kind: JoinKind,
) -> Result<Batch> {
    if keys.is_empty() || keys.len() > 64 {
        return Err(Error::Parse("JOIN USING expects 1 to 64 columns".into()));
    }
    if matches!(kind, JoinKind::Semi | JoinKind::Anti) {
        return Err(Error::Unsupported("JOIN USING for SEMI/ANTI joins".into()));
    }
    let mut seen = HashSet::new();
    for key in keys {
        if !seen.insert(key.to_ascii_lowercase()) {
            return Err(Error::Parse(format!("duplicate JOIN USING column: {key}")));
        }
    }
    let pairs = keys.iter().map(|key| (key.clone(), key.clone())).collect::<Vec<_>>();
    let joined = hash_join_multi(left, right, &pairs, kind)?;
    let left_width = left.schema().len();

    let mut output_fields = Vec::new();
    let mut positions = Vec::new();
    for key in keys {
        let l = left.schema().column(key)?;
        let r = right.schema().column(key)?;
        let left_field = left.schema().field(l)?;
        let right_field = right.schema().field(r)?;
        output_fields.push(Field::new(&left_field.name, left_field.data_type,
            left_field.nullable || right_field.nullable));
        // A coalesced key is required for RIGHT and FULL unmatched rows.
        positions.push((l, Some(left_width + r)));
    }
    for (index, field) in joined.schema().fields().iter().enumerate().take(left_width) {
        if !seen.contains(&field.name.to_ascii_lowercase()) {
            output_fields.push(field.clone());
            positions.push((index, None));
        }
    }
    for (index, field) in joined.schema().fields().iter().enumerate().skip(left_width) {
        let original = right.schema().field(index - left_width)?;
        if !seen.contains(&original.name.to_ascii_lowercase()) {
            output_fields.push(field.clone());
            positions.push((index, None));
        }
    }
    let schema = Schema::new(output_fields)?;
    let mut rows = Vec::with_capacity(joined.num_rows());
    for original in joined.rows()? {
        rows.push(positions.iter().map(|(left, right)| {
            let value = original[*left].clone();
            match right {
                Some(index) if value.is_null() => original[*index].clone(),
                _ => value,
            }
        }).collect());
    }
    Batch::from_rows(schema, rows)
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

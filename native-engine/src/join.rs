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
}

const MAX_JOIN_ROWS: usize = 1_000_000;

pub fn hash_join(
    left: &Batch,
    right: &Batch,
    left_key: &str,
    right_key: &str,
    kind: JoinKind,
) -> Result<Batch> {
    let left_column = left.schema().column(left_key)?;
    let right_column = right.schema().column(right_key)?;
    if left.schema().field(left_column)?.data_type != right.schema().field(right_column)?.data_type {
        return Err(Error::Type("join key types must match".into()));
    }
    let mut used: HashSet<String> = left
        .schema()
        .fields()
        .iter()
        .map(|field| field.name.to_ascii_lowercase())
        .collect();
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
        fields.push(Field::new(name, field.data_type, true));
    }
    let schema = Schema::new(fields)?;
    let mut right_hash: HashMap<Value, Vec<usize>> = HashMap::new();
    for row in 0..right.num_rows() {
        let key = right.value(row, right_column)?;
        if !key.is_null() {
            right_hash.entry(key.clone()).or_default().push(row);
        }
    }
    let mut output = Vec::new();
    let mut matched_right = vec![false; right.num_rows()];
    for left_row in 0..left.num_rows() {
        let key = left.value(left_row, left_column)?;
        let right_rows = if key.is_null() { None } else { right_hash.get(key) };
        if let Some(right_rows) = right_rows {
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
        for (right_row, matched) in matched_right.iter().enumerate() {
            if *matched { continue; }
            if output.len() >= MAX_JOIN_ROWS {
                return Err(Error::Execution("join output exceeds safety limit".into()));
            }
            let mut row = vec![Value::Null; left.schema().len()];
            row.extend(right.row(right_row)?);
            output.push(row);
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
}


#[cfg(test)]
mod outer_join_feature_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn right_and_full_join_preserve_unmatched_rows_and_nullability() {
        let left_schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let right_schema = Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap();
        let left = Batch::from_rows(left_schema, vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)]
        ]).unwrap();
        let right = Batch::from_rows(right_schema, vec![
            vec![Value::Integer(2)], vec![Value::Integer(3)], vec![Value::Null]
        ]).unwrap();
        let right_join = hash_join(&left, &right, "id", "id", JoinKind::Right).unwrap();
        assert_eq!(right_join.rows().unwrap(), vec![
            vec![Value::Integer(2), Value::Integer(2)],
            vec![Value::Null, Value::Integer(3)],
            vec![Value::Null, Value::Null],
        ]);
        assert!(right_join.schema().field(0).unwrap().nullable);
        let full = hash_join(&left, &right, "id", "id", JoinKind::Full).unwrap();
        assert_eq!(full.rows().unwrap(), vec![
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Integer(2)],
            vec![Value::Null, Value::Integer(3)],
            vec![Value::Null, Value::Null],
        ]);
    }
}

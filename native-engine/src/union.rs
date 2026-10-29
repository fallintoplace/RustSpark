use crate::{
    batch::Batch,
    schema::{Field, Schema},
    Error,
    Result,
};

fn compatible_union_schema(left: &Batch, right: &Batch) -> Result<Schema> {
    if left.schema().len() != right.schema().len() {
        return Err(Error::Schema("UNION column counts must match".into()));
    }
    let mut fields = Vec::new();
    for (a, b) in left.schema().fields().iter().zip(right.schema().fields()) {
        if a.data_type != b.data_type {
            return Err(Error::Type(format!("UNION type mismatch in column {}", a.name)));
        }
        fields.push(Field::new(
            &a.name,
            a.data_type,
            a.nullable || b.nullable,
        ));
    }
    Schema::new(fields)
}

pub fn union_all(left: &Batch, right: &Batch) -> Result<Batch> {
    let schema = compatible_union_schema(left, right)?;
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
    let schema = compatible_union_schema(left, right)?;
    let mut seen = std::collections::HashSet::new();
    let mut rows = Vec::new();
    for input in [left, right] {
        for index in 0..input.num_rows() {
            let row = input.row(index)?;
            if seen.insert(row.clone()) {
                rows.push(row);
            }
        }
    }
    Batch::from_rows(schema, rows)
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

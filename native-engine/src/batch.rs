use crate::{
    scalar::Value,
    schema::Schema,
    Error,
    Result,
};

/// A compact, typed columnar batch. Each column has the same row count.
#[derive(Clone, Debug, PartialEq)]
pub struct Batch {
    schema: Schema,
    columns: Vec<Vec<Value>>,
    rows: usize,
}

impl Batch {
    pub fn new(schema: Schema, columns: Vec<Vec<Value>>) -> Result<Self> {
        let rows = columns.first().map_or(0, Vec::len);
        Self::new_with_row_count(schema, columns, rows)
    }

    pub fn new_with_row_count(schema: Schema, columns: Vec<Vec<Value>>, rows: usize) -> Result<Self> {
        if columns.len() != schema.len() {
            return Err(Error::Schema("column count does not match schema".into()));
        }
        for (index, column) in columns.iter().enumerate() {
            if column.len() != rows {
                return Err(Error::Schema("all columns must have the same length".into()));
            }
            let field = schema.field(index)?;
            for value in column {
                if !field.data_type.accepts(value) {
                    return Err(Error::Type(format!("invalid value in column {}", field.name)));
                }
                if !field.nullable && value.is_null() {
                    return Err(Error::Type(format!("NULL in required column {}", field.name)));
                }
            }
        }
        Ok(Self { schema, columns, rows })
    }

    pub fn empty(schema: Schema) -> Self {
        let columns = vec![Vec::new(); schema.len()];
        Self {
            schema,
            columns,
            rows: 0,
        }
    }

    pub fn from_rows(schema: Schema, rows: Vec<Vec<Value>>) -> Result<Self> {
        let row_count = rows.len();
        let mut columns = vec![Vec::with_capacity(row_count); schema.len()];
        for row in rows {
            if row.len() != schema.len() {
                return Err(Error::Schema("row width does not match schema".into()));
            }
            for ((column, value), field) in columns.iter_mut().zip(row).zip(schema.fields()) {
                if !field.data_type.accepts(&value) {
                    return Err(Error::Type(format!("invalid value in column {}", field.name)));
                }
                if !field.nullable && value.is_null() {
                    return Err(Error::Type(format!("NULL in required column {}", field.name)));
                }
                column.push(value);
            }
        }
        Ok(Self { schema, columns, rows: row_count })
    }

    pub fn into_widened_schema(mut self, schema: Schema) -> Result<Self> {
        if schema.len() != self.schema.len() {
            return Err(Error::Schema("batch schema width mismatch".into()));
        }
        for (old, new) in self.schema.fields().iter().zip(schema.fields()) {
            if old.name != new.name || old.data_type != new.data_type ||
                (old.nullable && !new.nullable) {
                return Err(Error::Schema("cannot narrow or change batch field schema".into()));
            }
        }
        self.schema = schema;
        Ok(self)
    }

    pub(crate) fn relabel(self, schema: Schema) -> Result<Self> {
        if schema.len() != self.schema.len() {
            return Err(Error::Schema("column count does not match relabeled schema".into()));
        }
        for (index, (source, target)) in self.schema.fields().iter()
            .zip(schema.fields()).enumerate() {
            if source.data_type != target.data_type {
                return Err(Error::Type("cannot relabel column to a different type".into()));
            }
            if source.nullable && !target.nullable &&
                self.columns[index].iter().any(Value::is_null) {
                return Err(Error::Type(format!("NULL in required column {}", target.name)));
            }
        }
        Ok(Self { schema, ..self })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn num_rows(&self) -> usize {
        self.rows
    }

    pub fn column(&self, index: usize) -> Result<&[Value]> {
        self.columns
            .get(index)
            .map(Vec::as_slice)
            .ok_or_else(|| Error::Schema(format!("column index out of bounds: {index}")))
    }

    pub fn value(&self, row: usize, column: usize) -> Result<&Value> {
        self.column(column)?
            .get(row)
            .ok_or_else(|| Error::Execution(format!("row index out of bounds: {row}")))
    }

    pub fn row(&self, index: usize) -> Result<Vec<Value>> {
        if index >= self.rows {
            return Err(Error::Execution(format!("row index out of bounds: {index}")));
        }
        Ok(self.columns.iter().map(|column| column[index].clone()).collect())
    }

    pub fn take(&self, indices: &[usize]) -> Result<Self> {
        if let Some(&invalid) = indices.iter().find(|&&row| row >= self.rows) {
            return Err(Error::Execution(format!("row index out of bounds: {invalid}")));
        }
        if let Some(&start) = indices.first() {
            if indices.windows(2).all(|pair| pair[1] == pair[0] + 1) {
                return self.slice_rows(start, start + indices.len());
            }
        }
        let mut columns = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            let mut selected = Vec::with_capacity(indices.len());
            selected.extend(indices.iter().map(|&row| column[row].clone()));
            columns.push(selected);
        }
        // Source values already satisfy this batch's schema; row indices were checked above.
        Ok(Self { schema: self.schema.clone(), columns, rows: indices.len() })
    }

    pub fn rows(&self) -> Result<Vec<Vec<Value>>> {
        let mut rows = (0..self.rows)
            .map(|_| Vec::with_capacity(self.columns.len()))
            .collect::<Vec<_>>();
        for column in &self.columns {
            for (row, value) in rows.iter_mut().zip(column) {
                row.push(value.clone());
            }
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, Field};

    fn schema() -> Schema {
        Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap()
    }

    #[test]
    fn explicit_cardinality_supports_zero_column_batches() {
        let empty = Schema::new(vec![]).unwrap();
        let batch = Batch::new_with_row_count(empty.clone(), vec![], 3).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.rows().unwrap(), vec![vec![], vec![], vec![]]);
        assert_eq!(Batch::new(empty, vec![]).unwrap().num_rows(), 0);
        assert!(Batch::new_with_row_count(schema(), vec![vec![Value::Integer(1)]], 2).is_err());
    }

    #[test]
    fn rejects_ragged_and_nullable_violations() {
        assert!(Batch::new(schema(), vec![]).is_err());
        assert!(Batch::new(schema(), vec![vec![Value::Null]]).is_err());
    }

    #[test]
    fn rows_and_take_are_consistent() {
        let batch = Batch::from_rows(
            schema(),
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]],
        )
        .unwrap();
        assert_eq!(batch.take(&[1, 0]).unwrap().row(0).unwrap(), vec![Value::Integer(2)]);
        assert!(batch.take(&[2]).is_err());
    }
}

 
impl Batch {
    /// Slice contiguous rows while retaining the original schema.
    pub fn slice_rows(&self, start: usize, end: usize) -> Result<Self> {
        if start > end || end > self.num_rows() {
            return Err(Error::Execution("invalid batch row slice".into()));
        }
        let columns = self.columns.iter()
            .map(|column| column[start..end].to_vec())
            .collect();
        Ok(Self { schema: self.schema.clone(), columns, rows: end - start })
    }
}

#[cfg(test)]
mod batch_slicing_feature_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn slices_contiguous_rows_and_preserves_schema() {
        let schema = Schema::new(vec![Field::new("x", DataType::Integer, false)]).unwrap();
        let input = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Integer(3)]
        ]).unwrap();
        let sliced = input.slice_rows(1, 3).unwrap();
        assert_eq!(sliced.schema(), &schema);
        assert_eq!(sliced.rows().unwrap(), vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
        assert_eq!(input.slice_rows(1, 1).unwrap().num_rows(), 0);
        assert!(input.slice_rows(3, 4).is_err());
        assert!(input.slice_rows(2, 1).is_err());
    }
}


#[cfg(test)]
mod batch_slice_edge_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn empty_slices_keep_field_types_and_nullability() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("name", DataType::Text, true)]).unwrap(),
            vec![vec![Value::Text("first".into())], vec![Value::Null]],
        ).unwrap();
        let output = input.slice_rows(2, 2).unwrap();
        assert_eq!(output.num_rows(), 0);
        assert_eq!(output.schema(), input.schema());
        assert_eq!(input.slice_rows(1, 2).unwrap().row(0).unwrap(), vec![Value::Null]);
    }
}

#[cfg(test)]
mod zero_column_cardinality_consolidation_tests {
    use super::*;
    #[test]
    fn zero_width_rows_survive_construction_gather_and_slice() {
        let schema = Schema::new(vec![]).unwrap();
        let input = Batch::from_rows(schema.clone(), vec![vec![], vec![], vec![]]).unwrap();
        assert_eq!(input.num_rows(), 3);
        assert_eq!(input.rows().unwrap(), vec![vec![]; 3]);
        assert_eq!(input.take(&[2, 0, 2]).unwrap().rows().unwrap(), vec![vec![]; 3]);
        assert_eq!(input.slice_rows(1, 3).unwrap().rows().unwrap(), vec![vec![]; 2]);
        assert_eq!(input.slice_rows(2, 2).unwrap().num_rows(), 0);
        assert!(input.take(&[3]).is_err());
        assert!(input.slice_rows(0, 4).is_err());
        assert!(Batch::from_rows(schema, vec![vec![Value::Integer(1)]]).is_err());
    }
}

#[cfg(test)]
mod gathered_validity_fast_path_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn gather_preserves_nullable_values_and_duplicate_rows() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("payload", DataType::Text, true),
        ]).unwrap();
        let input = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Text("large payload".into())],
        ]).unwrap();
        let selected = input.take(&[1, 0, 1]).unwrap();
        assert_eq!(selected.schema(), &schema);
        assert_eq!(selected.rows().unwrap(), vec![
            vec![Value::Integer(2), Value::Text("large payload".into())],
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Text("large payload".into())],
        ]);
        assert!(input.take(&[2]).is_err());
    }
}

#[cfg(test)]
mod slice_fast_path_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    #[test]
    fn slicing_nullable_and_zero_width_rows_preserves_cardinality() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let input = Batch::from_rows(schema, vec![vec![Value::Null], vec![Value::Integer(5)]]).unwrap();
        assert_eq!(input.slice_rows(0, 1).unwrap().row(0).unwrap(), vec![Value::Null]);
        assert!(input.slice_rows(1, 3).is_err());
        let zero = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 4).unwrap();
        assert_eq!(zero.slice_rows(1, 3).unwrap().rows().unwrap(), vec![vec![], vec![]]);
    }
}

#[cfg(test)]
mod row_transpose_one_pass_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    #[test]
    fn validates_cells_during_transposition() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false), Field::new("s", DataType::Text, true)]).unwrap();
        assert!(Batch::from_rows(schema.clone(), vec![vec![Value::Null, Value::Null]]).is_err());
        assert!(Batch::from_rows(schema.clone(), vec![vec![Value::Integer(1), Value::Integer(2)]]).is_err());
        assert!(Batch::from_rows(schema.clone(), vec![vec![Value::Integer(1)]]).is_err());
        let good = Batch::from_rows(schema, vec![vec![Value::Integer(1), Value::Null]]).unwrap();
        assert_eq!(good.row(0).unwrap(), vec![Value::Integer(1), Value::Null]);
    }
}

#[cfg(test)]
mod schema_widen_without_copy_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    #[test]
    fn only_nullability_widening_is_allowed_and_data_is_unchanged() {
        let required = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let nullable = Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap();
        let batch = Batch::from_rows(required.clone(), vec![vec![Value::Integer(7)], vec![Value::Integer(9)]]).unwrap();
        assert_eq!(batch.clone().into_widened_schema(nullable.clone()).unwrap().rows().unwrap(), batch.rows().unwrap());
        assert_eq!(batch.clone().into_widened_schema(nullable.clone()).unwrap().schema(), &nullable);
        assert!(batch.clone().into_widened_schema(Schema::new(vec![Field::new("id", DataType::Text, true)]).unwrap()).is_err());
        assert!(batch.clone().into_widened_schema(Schema::new(vec![Field::new("other", DataType::Integer, true)]).unwrap()).is_err());
        let with_null = Batch::from_rows(nullable, vec![vec![Value::Null]]).unwrap();
        assert!(with_null.into_widened_schema(required).is_err());
        let zero = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 5).unwrap();
        assert_eq!(zero.into_widened_schema(Schema::new(vec![]).unwrap()).unwrap().num_rows(), 5);
    }
}

#[cfg(test)]
mod contiguous_take_fast_path_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn consecutive_gathers_and_arbitrary_gathers_match_expected_rows() {
        let input = Batch::from_rows(Schema::new(vec![
            Field::new("text", DataType::Text, true),
            Field::new("id", DataType::Integer, false),
        ]).unwrap(), vec![
            vec![Value::Text("a".into()), Value::Integer(0)],
            vec![Value::Null, Value::Integer(1)],
            vec![Value::Text("c".into()), Value::Integer(2)],
            vec![Value::Text("d".into()), Value::Integer(3)],
        ]).unwrap();
        assert_eq!(input.take(&[1, 2, 3]).unwrap(), input.slice_rows(1, 4).unwrap());
        assert_eq!(input.take(&[0]).unwrap(), input.slice_rows(0, 1).unwrap());
        assert_eq!(input.take(&[2, 0, 2]).unwrap().rows().unwrap(), vec![
            input.row(2).unwrap(), input.row(0).unwrap(), input.row(2).unwrap(),
        ]);
        assert_eq!(input.take(&[]).unwrap().num_rows(), 0);
        assert!(input.take(&[4]).is_err());
    }
}

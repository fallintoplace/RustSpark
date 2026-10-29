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
        if columns.len() != schema.len() {
            return Err(Error::Schema("column count does not match schema".into()));
        }
        let rows = columns.first().map_or(0, Vec::len);
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
        if schema.is_empty() && !rows.is_empty() {
            return Err(Error::Schema("nonempty rows require columns".into()));
        }
        let mut columns = vec![Vec::with_capacity(rows.len()); schema.len()];
        for row in rows {
            if row.len() != schema.len() {
                return Err(Error::Schema("row width does not match schema".into()));
            }
            for (column, value) in columns.iter_mut().zip(row) {
                column.push(value);
            }
        }
        Self::new(schema, columns)
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
        let mut columns = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            let mut selected = Vec::with_capacity(indices.len());
            for &row in indices {
                let value = column.get(row).ok_or_else(|| {
                    Error::Execution(format!("row index out of bounds: {row}"))
                })?;
                selected.push(value.clone());
            }
            columns.push(selected);
        }
        Self::new(self.schema.clone(), columns)
    }

    pub fn rows(&self) -> Result<Vec<Vec<Value>>> {
        (0..self.rows).map(|index| self.row(index)).collect()
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
        Self::new(self.schema.clone(), columns)
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

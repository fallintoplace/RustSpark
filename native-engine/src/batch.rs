use crate::{
    scalar::Value,
    schema::{Field, Schema},
    Error,
    Result,
};

fn validate_value(value: &Value, field: &Field) -> Result<()> {
    if !field.data_type.accepts(value) {
        return Err(Error::Type(format!("invalid value in column {}", field.name)));
    }
    if !field.nullable && value.is_null() {
        return Err(Error::Type(format!("NULL in required column {}", field.name)));
    }
    Ok(())
}

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
                validate_value(value, field)?;
            }
        }
        Ok(Self { schema, columns, rows })
    }

    /// Build a batch from columns whose values were already checked against
    /// the schema while decoding. Only trusted internal callers may use this:
    /// it validates the shape but deliberately skips a second cell scan.
    pub(crate) fn from_validated_columns(
        schema: Schema, columns: Vec<Vec<Value>>, rows: usize,
    ) -> Result<Self> {
        if columns.len() != schema.len() {
            return Err(Error::Schema("column count does not match schema".into()));
        }
        if columns.iter().any(|column| column.len() != rows) {
            return Err(Error::Schema("all columns must have the same length".into()));
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
                validate_value(&value, field)?;
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

    /// Consume validated column vectors without cloning their scalar payloads.
    /// Used by owned streaming operators that have already checked schemas.
    pub(crate) fn into_columns(self) -> Vec<Vec<Value>> {
        self.columns
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
            selected.extend(indices.iter().map(|&row| column[row].clone()));
            columns.push(selected);
        }
        // Source values already satisfy this batch's schema; row indices were checked above.
        Ok(Self { schema: self.schema.clone(), columns, rows: indices.len() })
    }

    /// Select rows from a columnar batch using a boolean mask.
    pub fn take_mask(&self, mask: &[bool]) -> Result<Self> {
        if mask.len() != self.rows {
            return Err(Error::Execution("selection mask length does not match batch".into()));
        }
        let selected = mask.iter().filter(|&&keep| keep).count();
        if selected == 0 { return Ok(Self::empty(self.schema.clone())); }
        let columns = self.columns.iter().map(|source| {
            let mut output = Vec::with_capacity(selected);
            output.extend(source.iter().zip(mask).filter_map(|(value, &keep)|
                keep.then(|| value.clone())
            ));
            output
        }).collect();
        Ok(Self { schema: self.schema.clone(), columns, rows: selected })
    }

    /// Gather monotonically increasing row indices from an owned batch.
    /// Moving retained scalar values avoids duplicating heap-backed TEXT data.
    pub(crate) fn take_sorted_owned(self, indices: &[usize]) -> Result<Self> {
        if indices.last().is_some_and(|&index| index >= self.rows) ||
            indices.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Error::Execution("owned batch gather indices must be increasing and in bounds".into()));
        }
        let mut columns = Vec::with_capacity(self.columns.len());
        for source in self.columns {
            let mut values = Vec::with_capacity(indices.len());
            let mut iter = source.into_iter();
            let mut consumed = 0usize;
            for &index in indices {
                // Vec::IntoIter skips dropped values and moves only the cells
                // needed by the output, without cloning String allocations.
                let value = iter.nth(index - consumed).expect("validated row index");
                values.push(value);
                consumed = index + 1;
            }
            columns.push(values);
        }
        Ok(Self { schema: self.schema, columns, rows: indices.len() })
    }

    /// Append a validated source row to owned column builders without making
    /// a temporary row Vec. Used by multiway external sort merging.
    pub(crate) fn append_row_to_columns(
        &self, row: usize, output: &mut [Vec<Value>],
    ) -> Result<()> {
        if row >= self.rows {
            return Err(Error::Execution(format!("row index out of bounds: {row}")));
        }
        if output.len() != self.columns.len() {
            return Err(Error::Schema("row append column count mismatch".into()));
        }
        for (target, source) in output.iter_mut().zip(&self.columns) {
            target.push(source[row].clone());
        }
        Ok(())
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


impl Batch {
    /// Apply a full row permutation in place. Record the old-to-new mapping
    /// once and replay its swap cycles over every column. No TEXT cell is cloned.
    pub(crate) fn permute_rows_owned(mut self, indices: &[usize]) -> Result<Self> {
        if indices.len() != self.rows {
            return Err(Error::Execution("row permutation has incorrect length".into()));
        }
        let mut destinations = vec![usize::MAX; self.rows];
        for (position, &source) in indices.iter().enumerate() {
            if source >= self.rows || destinations[source] != usize::MAX {
                return Err(Error::Execution("row permutation has duplicate or invalid indices".into()));
            }
            destinations[source] = position;
        }
        let mut swaps = Vec::new();
        for position in 0..self.rows {
            while destinations[position] != position {
                let target = destinations[position];
                destinations.swap(position, target);
                swaps.push((position, target));
            }
        }
        for column in &mut self.columns {
            for &(left, right) in &swaps {
                column.swap(left, right);
            }
        }
        Ok(self)
    }

    /// Slice an owned batch while moving values instead of cloning TEXT cells.
    /// The full-range case returns the original column vectors unchanged.
    pub fn slice_rows_owned(self, start: usize, end: usize) -> Result<Self> {
        if start > end || end > self.rows {
            return Err(Error::Execution("invalid batch row slice".into()));
        }
        if start == 0 && end == self.rows {
            return Ok(self);
        }
        let columns = self.columns.into_iter().map(|source| {
            source.into_iter().skip(start).take(end - start).collect()
        }).collect();
        Ok(Self { schema: self.schema, columns, rows: end - start })
    }
}

#[cfg(test)]
mod owned_batch_slices {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn partial_slice_moves_text_allocations_and_keeps_nulls() {
        let schema = Schema::new(vec![Field::new("payload", DataType::Text, true)]).unwrap();
        let input = Batch::from_rows(schema, vec![
            vec![Value::Null],
            vec![Value::Text("hello🙂".repeat(1024))],
            vec![Value::Text("last".into())],
        ]).unwrap();
        let ptr = match input.value(1, 0).unwrap() {
            Value::Text(text) => text.as_ptr(),
            _ => unreachable!(),
        };
        let expected = input.slice_rows(1, 2).unwrap();
        let owned = input.slice_rows_owned(1, 2).unwrap();
        assert_eq!(owned, expected);
        assert_eq!(match owned.value(0, 0).unwrap() {
            Value::Text(text) => text.as_ptr(),
            _ => unreachable!(),
        }, ptr);
    }

    #[test]
    fn validates_ranges_and_keeps_zero_width_cardinality() {
        let input = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 5).unwrap();
        assert_eq!(input.clone().slice_rows_owned(2, 4).unwrap().num_rows(), 2);
        assert_eq!(input.clone().slice_rows_owned(0, 5).unwrap(), input);
        assert!(input.clone().slice_rows_owned(4, 2).is_err());
        assert!(input.slice_rows_owned(4, 6).is_err());
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
mod boolean_selection_mask_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn mask_preserves_order_and_null_values() {
        let input = Batch::from_rows(Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("name", DataType::Text, true),
        ]).unwrap(), vec![
            vec![Value::Integer(1), Value::Text("A".into())],
            vec![Value::Integer(2), Value::Null],
            vec![Value::Integer(3), Value::Text("C".into())],
        ]).unwrap();
        assert_eq!(input.take_mask(&[false, true, true]).unwrap(),
            input.take(&[1, 2]).unwrap());
        assert_eq!(input.take_mask(&[false, false, false]).unwrap().num_rows(), 0);
        assert!(input.take_mask(&[true]).is_err());
        let zero = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 3).unwrap();
        let result = zero.take_mask(&[true, false, true]).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.rows().unwrap(), vec![vec![], vec![]]);
    }
}

#[cfg(test)]
mod batch_take_mask_no_match_tests {
    use super::*;
    use crate::schema::{DataType, Field};
    #[test]
    fn empty_selection_preserves_schema_and_zero_width_cardinality() {
        let schema = Schema::new(vec![
            Field::new("value", DataType::Text, true)
        ]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("🙂".repeat(100_000))], vec![Value::Null]
        ]).unwrap();
        let empty = batch.take_mask(&[false, false]).unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.schema(), &schema);
        assert_eq!(empty.column(0).unwrap(), &[]);
        assert!(batch.take_mask(&[]).is_err());
        let empty_schema = Schema::new(vec![]).unwrap();
        let no_columns = Batch::new_with_row_count(empty_schema.clone(), vec![], 4).unwrap();
        assert_eq!(no_columns.take_mask(&[false; 4]).unwrap().num_rows(), 0);
        assert_eq!(no_columns.take_mask(&[true; 4]).unwrap().num_rows(), 4);
    }
}

#[cfg(test)]
mod owned_row_permutation_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn permutes_columns_without_cloning_text_or_changing_nullable_rows() {
        let schema = Schema::new(vec![
            Field::new("payload", DataType::Text, true),
            Field::new("id", DataType::Integer, false),
        ]).unwrap();
        let large = "東京🙂".repeat(4096);
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Text(large), Value::Integer(0)],
            vec![Value::Null, Value::Integer(1)],
            vec![Value::Text("last".into()), Value::Integer(2)],
        ]).unwrap();
        let ptr = match batch.value(0, 0).unwrap() {
            Value::Text(s) => s.as_ptr(), _ => unreachable!(),
        };
        let expected = batch.take(&[2, 0, 1]).unwrap();
        let permuted = batch.clone().permute_rows_owned(&[2, 0, 1]).unwrap();
        assert_eq!(permuted, expected);
        let owned = batch.permute_rows_owned(&[2, 0, 1]).unwrap();
        assert_eq!(match owned.value(1, 0).unwrap() {
            Value::Text(s) => s.as_ptr(), _ => unreachable!(),
        }, ptr);
    }

    #[test]
    fn rejects_invalid_mappings_and_handles_empty_schema() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)],
        ]).unwrap();
        for indices in [&[0usize][..], &[0, 0][..], &[1, 2][..]] {
            assert!(batch.clone().permute_rows_owned(indices).is_err());
        }
        assert_eq!(batch.clone().permute_rows_owned(&[0, 1]).unwrap(), batch);
        let width0 = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 3).unwrap();
        assert_eq!(width0.permute_rows_owned(&[2, 0, 1]).unwrap().num_rows(), 3);
    }
}

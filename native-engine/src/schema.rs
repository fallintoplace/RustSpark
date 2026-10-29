use std::collections::HashSet;

use crate::{scalar::Value, Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    Text,
}

impl DataType {
    pub fn accepts(self, value: &Value) -> bool {
        matches!(
            (self, value),
            (_, Value::Null)
                | (Self::Boolean, Value::Boolean(_))
                | (Self::Integer, Value::Integer(_))
                | (Self::Float, Value::Float(_))
                | (Self::Text, Value::Text(_))
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Field {
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    fields: Vec<Field>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Result<Self> {
        let mut names = HashSet::new();
        for field in &fields {
            if field.name.trim().is_empty() {
                return Err(Error::Schema("column names cannot be empty".into()));
            }
            if !names.insert(field.name.to_ascii_lowercase()) {
                return Err(Error::Schema(format!("duplicate column: {}", field.name)));
            }
        }
        Ok(Self { fields })
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn column(&self, name: &str) -> Result<usize> {
        self.fields
            .iter()
            .position(|field| field.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| Error::Schema(format!("unknown column: {name}")))
    }

    pub fn field(&self, index: usize) -> Result<&Field> {
        self.fields
            .get(index)
            .ok_or_else(|| Error::Schema(format!("column index out of bounds: {index}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_must_be_unique_ignoring_ascii_case() {
        let result = Schema::new(vec![
            Field::new("city", DataType::Text, false),
            Field::new("CITY", DataType::Text, true),
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn lookups_accept_case_insensitive_names() {
        let schema = Schema::new(vec![Field::new("Revenue", DataType::Integer, false)]).unwrap();
        assert_eq!(schema.column("revenue").unwrap(), 0);
        assert!(schema.column("unknown").is_err());
    }
}

 
impl Schema {
    /// Retain named fields in the requested order; reject unknown and duplicate names.
    pub fn project_columns(&self, names: &[&str]) -> Result<Self> {
        Self::new(names.iter().map(|name| {
            Ok(self.field(self.column(name)?)?.clone())
        }).collect::<Result<Vec<_>>>()?)
    }
}

#[cfg(test)]
mod schema_projection_feature_tests {
    use super::*;

    #[test]
    fn projections_preserve_types_nullability_and_user_order() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("tag", DataType::Text, true),
        ]).unwrap();
        let projected = schema.project_columns(&["TAG", "id"]).unwrap();
        assert_eq!(projected.fields(), &[
            Field::new("tag", DataType::Text, true),
            Field::new("id", DataType::Integer, false),
        ]);
        assert!(schema.project_columns(&["id", "ID"]).is_err());
        assert!(schema.project_columns(&["missing"]).is_err());
    }
}


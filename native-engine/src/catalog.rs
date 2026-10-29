use std::collections::HashMap;

use crate::{batch::Batch, Error, Result};

#[derive(Clone, Debug, Default)]
pub struct Catalog {
    tables: HashMap<String, Batch>,
}

impl Catalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, name: &str, batch: Batch) -> Result<()> {
        let normalized = normalize(name)?;
        if self.tables.contains_key(&normalized) {
            return Err(Error::Schema(format!("table already exists: {name}")));
        }
        let _ = self.tables.insert(normalized, batch);
        Ok(())
    }

    pub fn replace(&mut self, name: &str, batch: Batch) -> Result<()> {
        let normalized = normalize(name)?;
        let _ = self.tables.insert(normalized, batch);
        Ok(())
    }

    /// Rename a registered table only when the target does not already exist.
    /// Validation happens before removing the source.
    pub fn rename(&mut self, source: &str, destination: &str) -> Result<()> {
        let from = normalize(source)?;
        let to = normalize(destination)?;
        if !self.tables.contains_key(&from) {
            return Err(Error::Schema(format!("unknown table: {source}")));
        }
        if from == to { return Ok(()); }
        if self.tables.contains_key(&to) {
            return Err(Error::Schema(format!("table already exists: {destination}")));
        }
        let batch = self.tables.remove(&from)
            .ok_or_else(|| Error::Schema(format!("unknown table: {source}")))?;
        self.tables.insert(to, batch);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Result<&Batch> {
        let normalized = normalize(name)?;
        self.tables
            .get(&normalized)
            .ok_or_else(|| Error::Schema(format!("unknown table: {name}")))
    }

    pub fn list(&self) -> Vec<&str> {
        let mut result: Vec<_> = self.tables.keys().map(String::as_str).collect();
        result.sort_unstable();
        result
    }
}

fn normalize(name: &str) -> Result<String> {
    if name.trim().is_empty() || name.chars().any(char::is_whitespace) {
        return Err(Error::Schema("expected a simple nonempty table name".into()));
    }
    Ok(name.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Schema;

    #[test]
    fn lookups_are_case_insensitive_but_duplicates_fail() {
        let mut catalog = Catalog::new();
        catalog.register("Sales", Batch::empty(Schema::new(vec![]).unwrap())).unwrap();
        assert!(catalog.get("sales").is_ok());
        assert!(catalog.register("SALES", Batch::empty(Schema::new(vec![]).unwrap())).is_err());
        assert_eq!(catalog.list(), ["sales"]);
    }
}

 
impl Catalog {
    /// Remove a registered table and return its data to the caller.
    pub fn unregister(&mut self, name: &str) -> Result<Batch> {
        let normalized = normalize(name)?;
        self.tables.remove(&normalized)
            .ok_or_else(|| Error::Schema(format!("unknown table: {name}")))
    }
}

#[cfg(test)]
mod catalog_unregister_feature_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn temporary_tables_can_be_removed_case_insensitively() {
        let mut catalog = Catalog::new();
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        catalog.register("Temp", Batch::empty(schema.clone())).unwrap();
        assert_eq!(catalog.unregister("TEMP").unwrap().schema(), &schema);
        assert!(catalog.get("temp").is_err());
        assert!(catalog.unregister("temp").is_err());
        catalog.register("temp", Batch::empty(schema)).unwrap();
    }
}


#[cfg(test)]
mod catalog_rename_engine_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn failed_rename_preserves_both_tables_and_data() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("source", Batch::from_rows(
            schema.clone(), vec![vec![Value::Integer(7)]],
        ).unwrap()).unwrap();
        catalog.register("taken", Batch::empty(schema)).unwrap();
        assert!(catalog.rename("source", "taken").is_err());
        assert!(catalog.rename("source", "white space").is_err());
        assert!(catalog.rename("missing", "future").is_err());
        assert_eq!(catalog.get("source").unwrap().row(0).unwrap(), vec![Value::Integer(7)]);
        catalog.rename("SOURCE", "renamed").unwrap();
        assert!(catalog.get("source").is_err());
        assert_eq!(catalog.get("RENAMED").unwrap().row(0).unwrap(), vec![Value::Integer(7)]);
        catalog.rename("renamed", "RENAMED").unwrap();
    }
}

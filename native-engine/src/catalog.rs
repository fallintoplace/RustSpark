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


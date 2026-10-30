use std::{borrow::Cow, collections::HashMap};

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
        // Register once through the entry API instead of probing the table
        // for existence and hashing the same key again for insertion.
        match self.tables.entry(normalized.into_owned()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(batch);
                Ok(())
            }
            std::collections::hash_map::Entry::Occupied(_) =>
                Err(Error::Schema(format!("table already exists: {name}"))),
        }
    }

    pub fn replace(&mut self, name: &str, batch: Batch) -> Result<()> {
        let normalized = normalize(name)?;
        let _ = self.tables.insert(normalized.into_owned(), batch);
        Ok(())
    }

    /// Rename a registered table only when the target does not already exist.
    /// Validation happens before removing the source.
    pub fn rename(&mut self, source: &str, destination: &str) -> Result<()> {
        let from = normalize(source)?;
        let to = normalize(destination)?;
        if !self.tables.contains_key(from.as_ref()) {
            return Err(Error::Schema(format!("unknown table: {source}")));
        }
        if from == to { return Ok(()); }
        if self.tables.contains_key(to.as_ref()) {
            return Err(Error::Schema(format!("table already exists: {destination}")));
        }
        let batch = self.tables.remove(from.as_ref())
            .ok_or_else(|| Error::Schema(format!("unknown table: {source}")))?;
        let _ = self.tables.insert(to.into_owned(), batch);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Result<&Batch> {
        let normalized = normalize(name)?;
        self.tables
            .get(normalized.as_ref())
            .ok_or_else(|| Error::Schema(format!("unknown table: {name}")))
    }

    pub fn list(&self) -> Vec<&str> {
        let mut result: Vec<_> = self.tables.keys().map(String::as_str).collect();
        result.sort_unstable();
        result
    }
}

fn normalize(name: &str) -> Result<Cow<'_, str>> {
    if !name.as_bytes().first().is_some_and(|byte|
        byte.is_ascii_alphabetic() || *byte == b'_'
    ) || !name.bytes().all(|byte|
        byte.is_ascii_alphanumeric() || byte == b'_'
    ) {
        return Err(Error::Schema("expected a simple ASCII table identifier".into()));
    }
    if name.bytes().any(|byte| byte.is_ascii_uppercase()) {
        Ok(Cow::Owned(name.to_ascii_lowercase()))
    } else {
        Ok(Cow::Borrowed(name))
    }
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
        self.tables.remove(normalized.as_ref())
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

#[cfg(test)]
mod single_probe_catalog_register_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    #[test]
    fn duplicate_registration_keeps_the_original_table() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let mut catalog = Catalog::new();
        let original = Batch::from_rows(schema.clone(), vec![vec![Value::Integer(10)]]).unwrap();
        let replacement = Batch::from_rows(schema, vec![vec![Value::Integer(99)]]).unwrap();
        catalog.register("Sales", original).unwrap();
        assert!(catalog.register("sales", replacement).is_err());
        assert_eq!(catalog.get("SALES").unwrap().row(0).unwrap(), vec![Value::Integer(10)]);
    }
}

#[cfg(test)]
mod borrowed_catalog_key_tests {
    use super::*;
    #[test]
    fn borrows_normalized_keys_and_preserves_case_insensitive_lookups() {
        assert!(matches!(normalize("sales").unwrap(), Cow::Borrowed(_)));
        assert!(matches!(normalize("Sales").unwrap(), Cow::Owned(_)));
        let mut catalog = Catalog::new();
        let batch = Batch::empty(crate::schema::Schema::new(vec![]).unwrap());
        catalog.register("SALES", batch).unwrap();
        assert!(catalog.get("sales").is_ok());
        assert!(catalog.get("Sales").is_ok());
        assert!(catalog.get("unknown").is_err());
        assert!(normalize("has space").is_err());
    }
}

#[cfg(test)]
mod catalog_identifier_validation_tests {
    use super::*;

    #[test]
    fn catalog_rejects_names_that_native_sql_cannot_reference() {
        for name in ["", "has space", "name.with.dot", "name;drop", "9leading", "x-y", "é"] {
            let mut catalog = Catalog::new();
            let input = Batch::empty(crate::schema::Schema::new(vec![]).unwrap());
            assert!(catalog.register(name, input).is_err(), "{name}");
        }
    }

    #[test]
    fn catalog_accepts_case_insensitive_sql_identifiers() {
        let mut catalog = Catalog::new();
        catalog.register("_Q1", Batch::empty(crate::schema::Schema::new(vec![]).unwrap())).unwrap();
        assert!(catalog.get("_q1").is_ok());
        assert!(catalog.rename("_Q1", "Table_2").is_ok());
        assert_eq!(catalog.list(), ["table_2"]);
    }
}

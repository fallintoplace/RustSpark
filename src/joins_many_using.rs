//! Lazy multi-table joins with one merged copy of each key.

use std::collections::HashSet;

use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::JoinType,
    prelude::DataFrame,
};

use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};

fn multi_using_operator(kind: JoinType) -> Result<&'static str> {
    match kind {
        JoinType::Inner => Ok("INNER"),
        JoinType::Left => Ok("LEFT"),
        JoinType::Right => Ok("RIGHT"),
        JoinType::Full => Ok("FULL OUTER"),
        _ => Err(DataFusionError::Plan(
            "join_many_tables_using supports inner and outer joins only".into(),
        )),
    }
}

fn validate_using_source_columns(
    names: &[String],
    columns: &[&str],
    table: &str,
) -> Result<HashSet<String>> {
    let mut seen: HashSet<String> = HashSet::new();
    for name in names {
        if !seen.insert(name.to_ascii_lowercase()) {
            return Err(DataFusionError::Plan(format!(
                "Duplicate source column name: {name}"
            )));
        }
    }
    for column in columns {
        if !names.iter().any(|name| name == column) {
            return Err(DataFusionError::Plan(format!(
                "Join column {column} does not exist in table {table}"
            )));
        }
    }
    Ok(seen)
}

impl RustSparkSession {
    /// Join at least two registered tables on shared, identically named keys.
    ///
    /// Joins are evaluated from left to right; each key appears once in the
    /// returned schema. For FULL and RIGHT joins, the key retains values from
    /// unmatched rows by coalescing both inputs at each step. Other output
    /// column names must be unique across tables. This operation remains lazy.
    pub async fn join_many_tables_using(
        &self,
        tables: &[&str],
        join_type: JoinType,
        columns: &[&str],
    ) -> Result<DataFrame> {
        if tables.len() < 2 {
            return Err(DataFusionError::Plan(
                "join_many_tables_using requires at least two tables".into(),
            ));
        }
        if columns.is_empty() || columns.iter().any(|column| column.trim().is_empty()) {
            return Err(DataFusionError::Plan(
                "join_many_tables_using requires non-empty key columns".into(),
            ));
        }
        let operator = multi_using_operator(join_type)?;
        let mut key_names = HashSet::new();
        for column in columns {
            if !key_names.insert(column.to_ascii_lowercase()) {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate multi-way USING key: {column}"
                )));
            }
        }
        let first = tables[0];
        let mut names: Vec<String> = self
            .table(first)
            .await?
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect();
        let mut seen = validate_using_source_columns(&names, columns, first)?;
        let mut query = format!(
            "SELECT * FROM {}",
            quote_catalog_table(first, "Invalid multi-way USING table name")?
        );
        let left = "\"__rustspark_using_left\"";
        let right = "\"__rustspark_using_right\"";
        for table in &tables[1..] {
            let right_names: Vec<String> = self
                .table(table)
                .await?
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect();
            validate_using_source_columns(&right_names, columns, table)?;
            let mut new_names = Vec::new();
            for name in &right_names {
                if !key_names.contains(&name.to_ascii_lowercase()) {
                    if !seen.insert(name.to_ascii_lowercase()) {
                        return Err(DataFusionError::Plan(format!(
                            "Multi-way USING would create duplicate non-key column: {name}"
                        )));
                    }
                    new_names.push(name.as_str());
                }
            }

            let mut projection = Vec::with_capacity(columns.len() + names.len() + new_names.len());
            let mut predicates = Vec::with_capacity(columns.len());
            for key in columns {
                let col = quote_join_identifier(key);
                projection.push(format!("COALESCE({left}.{col}, {right}.{col}) AS {col}"));
                predicates.push(format!("{left}.{col} = {right}.{col}"));
            }
            for name in &names {
                if !key_names.contains(&name.to_ascii_lowercase()) {
                    let col = quote_join_identifier(name);
                    projection.push(format!("{left}.{col} AS {col}"));
                }
            }
            for name in &new_names {
                let col = quote_join_identifier(name);
                projection.push(format!("{right}.{col} AS {col}"));
            }
            query = format!(
                "SELECT {} FROM ({query}) AS {left} {operator} JOIN {} AS {right} ON {}",
                projection.join(", "),
                quote_catalog_table(table, "Invalid multi-way USING table name")?,
                predicates.join(" AND ")
            );
            names = projection_names(columns, &names, &right_names, &key_names);
        }
        self.sql(&query).await
    }
}

fn projection_names(
    keys: &[&str],
    previous: &[String],
    right: &[String],
    key_names: &HashSet<String>,
) -> Vec<String> {
    let mut output: Vec<String> = keys.iter().map(|name| (*name).to_owned()).collect();
    let mut used: HashSet<String> = key_names.clone();
    for name in previous.iter().chain(right) {
        if used.insert(name.to_ascii_lowercase()) {
            output.push(name.clone());
        }
    }
    output
}

#[cfg(test)]
mod column_validation_tests {
    use super::validate_using_source_columns;

    #[test]
    fn detects_duplicate_and_missing_source_columns() {
        let names = vec!["id".to_owned(), "payload".to_owned()];
        let seen = validate_using_source_columns(&names, &["id"], "items").unwrap();
        assert!(seen.contains("payload"));
        assert!(validate_using_source_columns(&names, &["missing"], "items").is_err());
        let duplicate = vec!["id".to_owned(), "ID".to_owned()];
        assert!(validate_using_source_columns(&duplicate, &["id"], "items").is_err());
    }
}

use datafusion::{
    error::{DataFusionError, Result},
    prelude::DataFrame,
};

use crate::RustSparkSession;

// Quote every component. Table names are identifiers, never executable SQL.
fn quote_table_path(name: &str) -> Result<String> {
    let parts: Vec<_> = name.split('.').collect();
    if parts
        .iter()
        .any(|part| part.is_empty() || part.trim() != *part)
    {
        return Err(DataFusionError::Plan(
            "Set operations require a non-empty table path".into(),
        ));
    }
    Ok(parts
        .into_iter()
        .map(|part| format!("\"{}\"", part.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join("."))
}

impl RustSparkSession {
    async fn set_operation(
        &self,
        left: &str,
        right: &str,
        operator: &'static str,
    ) -> Result<DataFrame> {
        let left = quote_table_path(left)?;
        let right = quote_table_path(right)?;
        self.sql_read_only(&format!(
            "SELECT * FROM {left} {operator} SELECT * FROM {right}"
        ))
        .await
    }

    /// Keep each distinct row that occurs in both registered tables.
    ///
    /// Columns are matched by position and must have compatible types.
    /// The returned DataFrame is lazy; NULLs follow SQL set semantics.
    pub async fn intersect_tables(&self, left: &str, right: &str) -> Result<DataFrame> {
        self.set_operation(left, right, "INTERSECT").await
    }

    /// Keep distinct rows in the left table but not the right table.
    pub async fn except_tables(&self, left: &str, right: &str) -> Result<DataFrame> {
        self.set_operation(left, right, "EXCEPT").await
    }
}

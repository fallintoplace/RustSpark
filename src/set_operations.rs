use datafusion::{error::Result, prelude::DataFrame};

use crate::RustSparkSession;

impl RustSparkSession {
    async fn set_operation(
        &self,
        left: &str,
        right: &str,
        operator: &'static str,
    ) -> Result<DataFrame> {
        let left =
            super::quote_catalog_table(left, "Set operations require a non-empty table path")?;
        let right =
            super::quote_catalog_table(right, "Set operations require a non-empty table path")?;
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

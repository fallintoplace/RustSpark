use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{error::Result, prelude::DataFrame};

impl RustSparkSession {
    /// Count strictly positive values per group while ignoring NULLs.
    ///
    /// Grouped counts use DataFusion's native aggregate operators.
    pub async fn positive_counts_per_group(
        &self,
        table: &str,
        group_key: &str,
        value_column: &str,
    ) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[group_key, value_column])?;
        let source = quote_catalog_table(table, "expected a table path")?;
        let group_key = quote_join_identifier(group_key);
        let value_column = quote_join_identifier(value_column);
        self.sql_read_only(&format!(
            "SELECT {group_key} AS group_key, SUM(CASE WHEN {value_column} > 0 THEN 1 ELSE 0 END) AS positive_count FROM {source} GROUP BY {group_key}"
        ))
        .await
    }
}

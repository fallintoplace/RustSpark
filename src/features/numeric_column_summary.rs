use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{error::Result, prelude::DataFrame};

impl RustSparkSession {
    /// Return one summary row for a numeric column.
    ///
    /// Output fields are row_count, non_null_count, null_count, min_value,
    /// max_value, and mean_value. AVG and extrema return NULL when no
    /// non-NULL values exist. Non-numeric types are rejected by the planner.
    pub async fn numeric_column_summary(&self, table: &str, column: &str) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        let source = quote_catalog_table(table, "numeric_column_summary requires a table path")?;
        let key = quote_join_identifier(column);
        self.sql_read_only(&format!(
            "SELECT COUNT(*) AS row_count, COUNT({key}) AS non_null_count, \
             COUNT(*) - COUNT({key}) AS null_count, MIN({key}) AS min_value, \
             MAX({key}) AS max_value, AVG({key}) AS mean_value FROM {source}"
        ))
        .await
    }
}

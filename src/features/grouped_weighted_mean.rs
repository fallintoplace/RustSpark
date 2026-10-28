use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{error::Result, prelude::DataFrame};

impl RustSparkSession {
    /// Calculate weighted means, excluding NULL value or weight pairs.
    ///
    /// Returns a lazy DataFrame; this method never collects raw data rows.
    pub async fn weighted_mean_per_group(
        &self,
        table: &str,
        group_key: &str,
        value_column: &str,
        weight_column: &str,
    ) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[group_key, value_column, weight_column])?;
        let source = quote_catalog_table(table, "expected a table path")?;
        let group_key = quote_join_identifier(group_key);
        let value_column = quote_join_identifier(value_column);
        let weight_column = quote_join_identifier(weight_column);
        self.sql_read_only(&format!(
            "SELECT {group_key} AS group_key, SUM(CAST({value_column} AS DOUBLE) * CAST({weight_column} AS DOUBLE)) / NULLIF(SUM(CASE WHEN {value_column} IS NOT NULL AND {weight_column} IS NOT NULL THEN CAST({weight_column} AS DOUBLE) END), 0) AS weighted_mean FROM {source} GROUP BY {group_key}"
        ))
        .await
    }
}

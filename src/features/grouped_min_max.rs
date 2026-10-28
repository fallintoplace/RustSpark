use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{error::Result, prelude::DataFrame};

impl RustSparkSession {
    /// Get per-group extrema as a lazy aggregation.
    ///
    /// DataFusion performs the aggregation; this helper does not collect rows.
    pub async fn min_max_per_group(
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
            "SELECT {group_key} AS group_key, MIN({value_column}) AS min_value, MAX({value_column}) AS max_value FROM {source} GROUP BY {group_key}"
        ))
        .await
    }
}

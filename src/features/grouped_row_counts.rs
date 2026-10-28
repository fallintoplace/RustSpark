use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{error::Result, prelude::DataFrame};

impl RustSparkSession {
    /// Count rows by partition key without collecting source rows.
    ///
    /// DataFusion performs the aggregation; this helper does not collect rows.
    pub async fn row_counts_per_group(
        &self,
        table: &str,
        group_key: &str,
        value_column: &str,
    ) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[group_key, value_column])?;
        let source = quote_catalog_table(table, "expected a table path")?;
        let group_key = quote_join_identifier(group_key);
        self.sql_read_only(&format!(
            "SELECT {group_key} AS group_key, COUNT(*) AS row_count FROM {source} GROUP BY {group_key}"
        ))
        .await
    }
}

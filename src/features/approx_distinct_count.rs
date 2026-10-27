use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{error::Result, prelude::DataFrame};

impl RustSparkSession {
    /// Estimate the number of distinct non-NULL values without exact deduplication.
    ///
    /// The estimate is approximate. This returns a lazy one-row DataFrame
    /// with an `approximate_distinct` column.
    pub async fn approx_distinct_column_count(
        &self,
        table: &str,
        column: &str,
    ) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        let source = quote_catalog_table(table, "approx_distinct_column_count requires a table path")?;
        let key = quote_join_identifier(column);
        self.sql_read_only(&format!(
            "SELECT APPROX_DISTINCT({key}) AS approximate_distinct FROM {source}"
        ))
        .await
    }
}

use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{error::{Result}, prelude::DataFrame};

impl RustSparkSession {
    /// Estimate non-NULL distinct values per group with DataFusion's sketch aggregate.
    ///
    /// Computation stays in the DataFusion query plan. Estimates are not exact.
    pub async fn approx_distinct_per_group(
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
            "SELECT {group_key} AS group_key, APPROX_DISTINCT({value_column}) AS estimated_distinct FROM {source} GROUP BY {group_key}"
        ))
        .await
    }
}

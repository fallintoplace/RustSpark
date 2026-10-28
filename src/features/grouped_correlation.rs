use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{error::Result, prelude::DataFrame};

impl RustSparkSession {
    /// Calculate Pearson correlation for two numeric measures by group.
    ///
    /// Returns a lazy DataFrame; NULL pairs are ignored by the SQL aggregate.
    pub async fn correlation_per_group(
        &self,
        table: &str,
        group_key: &str,
        left_column: &str,
        right_column: &str,
    ) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[group_key, left_column, right_column])?;
        let source = quote_catalog_table(table, "expected a table path")?;
        let group_key = quote_join_identifier(group_key);
        let left_column = quote_join_identifier(left_column);
        let right_column = quote_join_identifier(right_column);
        self.sql_read_only(&format!(
            "SELECT {group_key} AS group_key, CORR(CAST({left_column} AS DOUBLE), CAST({right_column} AS DOUBLE)) AS correlation FROM {source} GROUP BY {group_key}"
        ))
        .await
    }
}

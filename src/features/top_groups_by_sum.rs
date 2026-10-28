use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{error::Result, prelude::DataFrame};

impl RustSparkSession {
    /// Return the highest-value groups using an aggregate and bounded ordering.
    ///
    /// Returns a lazy DataFrame without collecting groups in the caller.
    pub async fn top_groups_by_sum(
        &self,
        table: &str,
        group_key: &str,
        value_column: &str,
        limit: usize,
    ) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[group_key, value_column])?;
        let source = quote_catalog_table(table, "expected a table path")?;
        let group_key = quote_join_identifier(group_key);
        let value_column = quote_join_identifier(value_column);
        self.sql_read_only(&format!(
            "SELECT {group_key} AS group_key, SUM(CAST({value_column} AS DOUBLE)) AS total FROM {source} GROUP BY {group_key} ORDER BY total DESC NULLS LAST, group_key LIMIT {limit}"
        ))
        .await
    }
}

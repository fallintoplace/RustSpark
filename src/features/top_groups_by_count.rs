use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{error::Result, prelude::DataFrame};

impl RustSparkSession {
    /// Return the busiest groups using aggregate plus TopK planning.
    ///
    /// Returns a lazy DataFrame without collecting groups in the caller.
    pub async fn top_groups_by_count(
        &self,
        table: &str,
        group_key: &str,
        limit: usize,
    ) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[group_key])?;
        let source = quote_catalog_table(table, "expected a table path")?;
        let group_key = quote_join_identifier(group_key);
        self.sql_read_only(&format!(
            "SELECT {group_key} AS group_key, COUNT(*) AS row_count FROM {source} GROUP BY {group_key} ORDER BY row_count DESC, group_key LIMIT {limit}"
        ))
        .await
    }
}

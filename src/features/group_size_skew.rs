use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{error::Result, prelude::DataFrame};

impl RustSparkSession {
    /// Summarize group-size skew without collecting each group's counts in the caller.
    ///
    /// The grouped scan can be partitioned by DataFusion. Empty inputs return
    /// zero groups and NULL extrema, matching SQL aggregate semantics.
    pub async fn group_size_skew(&self, table: &str, column: &str) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        let source = quote_catalog_table(table, "group_size_skew requires a table path")?;
        let key = quote_join_identifier(column);
        self.sql_read_only(&format!(
            "SELECT COUNT(*) AS group_count, MIN(n) AS min_rows, MAX(n) AS max_rows, \
             AVG(n) AS mean_rows FROM (SELECT COUNT(*) AS n FROM {source} GROUP BY {key}) AS bins"
        ))
        .await
    }
}

use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{error::{Result}, prelude::DataFrame};

impl RustSparkSession {
    /// Estimate duplicates with a bounded-size approximate-distinct sketch.
    ///
    /// Uses DataFusion aggregates and returns a lazy DataFrame. Very high-cardinality
    /// histogram groups may still consume substantial engine memory.
    pub async fn approximate_duplicate_count(
        &self,
        table: &str,
        value_column: &str,
    ) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[value_column])?;
        let source = quote_catalog_table(table, "expected a table path")?;
        let value_column = quote_join_identifier(value_column);
        self.sql_read_only(&format!(
            "SELECT COUNT({value_column}) AS non_null_rows, APPROX_DISTINCT({value_column}) AS estimated_unique, CAST(COUNT({value_column}) AS BIGINT) - CAST(APPROX_DISTINCT({value_column}) AS BIGINT) AS estimated_duplicates FROM {source}"
        ))
        .await
    }
}

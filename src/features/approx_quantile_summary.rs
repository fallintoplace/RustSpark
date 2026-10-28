use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{error::{Result}, prelude::DataFrame};

impl RustSparkSession {
    /// Return approximate first, second, and third quartiles in one query.
    ///
    /// Uses DataFusion aggregates and returns a lazy DataFrame. Very high-cardinality
    /// histogram groups may still consume substantial engine memory.
    pub async fn approximate_quantile_summary(
        &self,
        table: &str,
        value_column: &str,
    ) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[value_column])?;
        let source = quote_catalog_table(table, "expected a table path")?;
        let value_column = quote_join_identifier(value_column);
        self.sql_read_only(&format!(
            "SELECT APPROX_PERCENTILE_CONT(CAST({value_column} AS DOUBLE), 0.25) AS q25, APPROX_PERCENTILE_CONT(CAST({value_column} AS DOUBLE), 0.50) AS q50, APPROX_PERCENTILE_CONT(CAST({value_column} AS DOUBLE), 0.75) AS q75 FROM {source}"
        ))
        .await
    }
}

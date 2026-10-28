use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{error::{DataFusionError, Result}, prelude::DataFrame};

impl RustSparkSession {
    /// Estimate a per-group percentile without sorting each group in the caller.
    ///
    /// Computation stays in the DataFusion query plan. Estimates are not exact.
    pub async fn approx_percentile_per_group(
        &self,
        table: &str,
        group_key: &str,
        value_column: &str,
        percentile: f64,
    ) -> Result<DataFrame> {
        if !percentile.is_finite() || !(0.0..=1.0).contains(&percentile) {
            return Err(DataFusionError::Plan(
                "percentile must be finite and between 0 and 1".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[group_key, value_column])?;
        let source = quote_catalog_table(table, "expected a table path")?;
        let group_key = quote_join_identifier(group_key);
        let value_column = quote_join_identifier(value_column);
        self.sql_read_only(&format!(
            "SELECT {group_key} AS group_key, APPROX_PERCENTILE_CONT(CAST({value_column} AS DOUBLE), {percentile}) AS estimated_percentile FROM {source} GROUP BY {group_key}"
        ))
        .await
    }
}

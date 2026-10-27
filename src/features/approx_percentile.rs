use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{error::{DataFusionError, Result}, prelude::DataFrame};

impl RustSparkSession {
    /// Estimate a numeric percentile between 0.0 (minimum) and 1.0 (maximum).
    ///
    /// The result is a lazy one-row DataFrame with an `estimated_percentile`
    /// column. This is an approximation, not an exact median calculation.
    pub async fn approx_column_percentile(
        &self,
        table: &str,
        column: &str,
        percentile: f64,
    ) -> Result<DataFrame> {
        if !percentile.is_finite() || !(0.0..=1.0).contains(&percentile) {
            return Err(DataFusionError::Plan(
                "approx_column_percentile requires a finite value from 0 to 1".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        let source = quote_catalog_table(table, "approx_column_percentile requires a table path")?;
        let key = quote_join_identifier(column);
        self.sql_read_only(&format!(
            "SELECT APPROX_PERCENTILE_CONT(CAST({key} AS DOUBLE), {percentile}) AS estimated_percentile FROM {source}"
        ))
        .await
    }
}

use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{
    error::{DataFusionError, Result},
    prelude::DataFrame,
};

impl RustSparkSession {
    /// Bucket numeric values lazily using a caller-selected positive bucket width.
    ///
    /// Uses DataFusion aggregates and returns a lazy DataFrame. Very high-cardinality
    /// histogram groups may still consume substantial engine memory.
    pub async fn fixed_width_histogram(
        &self,
        table: &str,
        value_column: &str,
        bucket_width: f64,
    ) -> Result<DataFrame> {
        if !bucket_width.is_finite() || bucket_width <= 0.0 {
            return Err(DataFusionError::Plan(
                "bucket_width must be finite and strictly positive".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[value_column])?;
        let source = quote_catalog_table(table, "expected a table path")?;
        let value_column = quote_join_identifier(value_column);
        self.sql_read_only(&format!(
            "SELECT FLOOR(CAST({value_column} AS DOUBLE) / {bucket_width}) AS bucket_id, COUNT(*) AS row_count FROM {source} WHERE {value_column} IS NOT NULL GROUP BY FLOOR(CAST({value_column} AS DOUBLE) / {bucket_width}) ORDER BY bucket_id"
        ))
        .await
    }
}

use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{common::ScalarValue, error::Result, prelude::DataFrame};

impl RustSparkSession {
    /// Filter rows by NULL-safe equality, including matching two NULLs.
    ///
    /// Values remain typed SQL parameters. Unlike ordinary equality, a NULL
    /// value selects rows where the source column is NULL.
    pub async fn filter_rows_null_safe_equal(
        &self,
        table: &str,
        column: &str,
        value: ScalarValue,
    ) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        let source =
            quote_catalog_table(table, "filter_rows_null_safe_equal requires a table path")?;
        let key = quote_join_identifier(column);
        self.sql_read_only_with_params(
            &format!("SELECT * FROM {source} WHERE {key} IS NOT DISTINCT FROM $1"),
            vec![value],
        )
        .await
    }
}

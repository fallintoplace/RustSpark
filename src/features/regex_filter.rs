use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{common::ScalarValue, error::Result, prelude::DataFrame};

impl RustSparkSession {
    /// Filter a text column with a regular expression without interpolating the pattern.
    ///
    /// NULL values do not match. Invalid regular expressions are reported by
    /// DataFusion when the query is planned or executed.
    pub async fn filter_rows_regex(
        &self,
        table: &str,
        column: &str,
        pattern: &str,
    ) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        let source = quote_catalog_table(table, "filter_rows_regex requires a table path")?;
        let key = quote_join_identifier(column);
        self.sql_read_only_with_params(
            &format!("SELECT * FROM {source} WHERE regexp_like({key}, $1)"),
            vec![ScalarValue::Utf8(Some(pattern.to_owned()))],
        )
        .await
    }
}

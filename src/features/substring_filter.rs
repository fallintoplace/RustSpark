use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{common::ScalarValue, error::Result, prelude::DataFrame};

impl RustSparkSession {
    /// Keep rows where a text column contains a literal substring.
    ///
    /// Matching is case-sensitive unless `case_sensitive` is false. SQL
    /// wildcard characters are literal text, not LIKE pattern operators.
    /// NULL values do not match and the input table stays unchanged.
    pub async fn filter_rows_contains(
        &self,
        table: &str,
        column: &str,
        needle: &str,
        case_sensitive: bool,
    ) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        let source = quote_catalog_table(table, "filter_rows_contains requires a table path")?;
        let key = quote_join_identifier(column);
        let predicate = if case_sensitive {
            format!("STRPOS({key}, $1) > 0")
        } else {
            format!("STRPOS(LOWER({key}), LOWER($1)) > 0")
        };
        self.sql_read_only_with_params(
            &format!("SELECT * FROM {source} WHERE {predicate}"),
            vec![ScalarValue::Utf8(Some(needle.to_owned()))],
        )
        .await
    }
}

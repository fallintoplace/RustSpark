use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{
    error::{DataFusionError, Result},
    prelude::DataFrame,
};

impl RustSparkSession {
    /// Reshape selected columns into name/value rows, preserving id columns.
    ///
    /// Values are cast to VARCHAR to permit mixed source column types. NULL
    /// values are retained. All output is lazy and input rows are unchanged.
    pub async fn unpivot_columns(
        &self,
        table: &str,
        id_columns: &[&str],
        value_columns: &[&str],
        name_column: &str,
        value_column: &str,
    ) -> Result<DataFrame> {
        if value_columns.is_empty() {
            return Err(DataFusionError::Plan(
                "unpivot_columns requires value columns".into(),
            ));
        }
        if name_column.trim().is_empty()
            || value_column.trim().is_empty()
            || name_column.trim() != name_column
            || value_column.trim() != value_column
            || name_column.eq_ignore_ascii_case(value_column)
            || id_columns.iter().any(|id| {
                id.eq_ignore_ascii_case(name_column) || id.eq_ignore_ascii_case(value_column)
            })
        {
            return Err(DataFusionError::Plan(
                "unpivot_columns output names must be distinct from id columns".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        let selected = id_columns
            .iter()
            .chain(value_columns.iter())
            .copied()
            .collect::<Vec<_>>();
        Self::validate_selected_columns(&dataframe, &selected)?;
        let source = quote_catalog_table(table, "unpivot_columns requires a table path")?;
        let ids = id_columns
            .iter()
            .map(|id| quote_join_identifier(id))
            .collect::<Vec<_>>();
        let id_prefix = if ids.is_empty() {
            String::new()
        } else {
            format!("{}, ", ids.join(", "))
        };
        let label_alias = quote_join_identifier(name_column);
        let value_alias = quote_join_identifier(value_column);
        let queries = value_columns.iter().map(|name| {
            let label = name.replace('\'', "''");
            let key = quote_join_identifier(name);
            format!("SELECT {id_prefix}'{label}' AS {label_alias}, CAST({key} AS VARCHAR) AS {value_alias} FROM {source}")
        }).collect::<Vec<_>>();
        self.sql_read_only(&queries.join(" UNION ALL ")).await
    }
}

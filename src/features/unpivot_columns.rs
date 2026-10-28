use std::fmt::Write as _;

use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{
    error::{DataFusionError, Result},
    prelude::DataFrame,
};

fn validate_unpivot_names(
    id_columns: &[&str],
    name_column: &str,
    value_column: &str,
) -> Result<()> {
    if name_column.trim().is_empty()
        || value_column.trim().is_empty()
        || name_column.trim() != name_column
        || value_column.trim() != value_column
        || name_column.eq_ignore_ascii_case(value_column)
        || id_columns
            .iter()
            .any(|id| id.eq_ignore_ascii_case(name_column) || id.eq_ignore_ascii_case(value_column))
    {
        return Err(DataFusionError::Plan(
            "unpivot_columns output names must be distinct from id columns".into(),
        ));
    }
    Ok(())
}

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
        validate_unpivot_names(id_columns, name_column, value_column)?;
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
        // Reserve room for one SELECT per value column and the separators.
        let per_branch = 64usize
            .saturating_add(id_prefix.len())
            .saturating_add(label_alias.len())
            .saturating_add(value_alias.len())
            .saturating_add(source.len());
        let mut query = String::with_capacity(value_columns.len().saturating_mul(per_branch));
        for (index, name) in value_columns.iter().enumerate() {
            if index != 0 {
                query.push_str(" UNION ALL ");
            }
            let label = name.replace('\'', "''");
            let key = quote_join_identifier(name);
            write!(
                &mut query,
                "SELECT {id_prefix}'{label}' AS {label_alias}, CAST({key} AS VARCHAR) AS {value_alias} FROM {source}"
            )
            .expect("writing to a String cannot fail");
        }
        self.sql_read_only(&query).await
    }
}

#[cfg(test)]
mod tests {
    use super::validate_unpivot_names;

    #[test]
    fn rejects_conflicting_unpivot_output_names() {
        assert!(validate_unpivot_names(&["id"], "column", "value").is_ok());
        assert!(validate_unpivot_names(&["id"], "ID", "value").is_err());
        assert!(validate_unpivot_names(&["id"], "name", "NAME").is_err());
        assert!(validate_unpivot_names(&[], " name", "value").is_err());
    }
}

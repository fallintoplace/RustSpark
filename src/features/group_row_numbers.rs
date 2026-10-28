use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{
    error::{DataFusionError, Result},
    prelude::DataFrame,
};

impl RustSparkSession {
    /// Append one-based row numbers within each partition.
    ///
    /// A true sort direction orders ascending with NULLs first. Include a
    /// unique tie-breaker for deterministic numbers.
    pub async fn with_group_row_numbers(
        &self,
        table: &str,
        partitions: &[&str],
        order: &[(&str, bool)],
        output_column: &str,
    ) -> Result<DataFrame> {
        if partitions.is_empty() || order.is_empty() {
            return Err(DataFusionError::Plan(
                "with_group_row_numbers requires partition and sort keys".into(),
            ));
        }
        if output_column.trim().is_empty() || output_column.trim() != output_column {
            return Err(DataFusionError::Plan(
                "with_group_row_numbers requires a non-empty, unpadded output name".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, partitions)?;
        let keys = order.iter().map(|(name, _)| *name).collect::<Vec<_>>();
        Self::validate_selected_columns(&dataframe, &keys)?;
        if dataframe
            .schema()
            .fields()
            .iter()
            .any(|field| field.name().eq_ignore_ascii_case(output_column))
        {
            return Err(DataFusionError::Plan(format!(
                "Output column already exists: {output_column}"
            )));
        }
        let source = quote_catalog_table(table, "with_group_row_numbers requires a table path")?;
        let mut group_by = String::new();
        for name in partitions {
            if !group_by.is_empty() {
                group_by.push_str(", ");
            }
            group_by.push_str(&quote_join_identifier(name));
        }
        let mut ordering = String::new();
        for (name, ascending) in order {
            if !ordering.is_empty() {
                ordering.push_str(", ");
            }
            ordering.push_str(&quote_join_identifier(name));
            ordering.push_str(if *ascending {
                " ASC NULLS FIRST"
            } else {
                " DESC NULLS LAST"
            });
        }
        let output = quote_join_identifier(output_column);
        self.sql_read_only(&format!(
            "SELECT *, ROW_NUMBER() OVER (PARTITION BY {group_by} ORDER BY {ordering}) AS {output} FROM {source}"
        ))
        .await
    }
}

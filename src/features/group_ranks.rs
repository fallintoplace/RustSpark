use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{
    error::{DataFusionError, Result},
    prelude::DataFrame,
};

impl RustSparkSession {
    /// Append a rank inside each partition, preserving equal-key ties.
    ///
    /// Standard rank skips positions after ties (1, 1, 3), while dense rank
    /// does not (1, 1, 2). A true sort direction orders ascending with
    /// NULLs first, and ties are determined by all sort keys.
    pub async fn with_group_ranks(
        &self,
        table: &str,
        partitions: &[&str],
        order: &[(&str, bool)],
        output_column: &str,
        dense: bool,
    ) -> Result<DataFrame> {
        if partitions.is_empty() || order.is_empty() {
            return Err(DataFusionError::Plan(
                "with_group_ranks requires partition and sort keys".into(),
            ));
        }
        if output_column.trim().is_empty() || output_column.trim() != output_column {
            return Err(DataFusionError::Plan(
                "with_group_ranks requires a non-empty, unpadded output name".into(),
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
        let source = quote_catalog_table(table, "with_group_ranks requires a table path")?;
        let mut partition_by = String::new();
        for name in partitions {
            if !partition_by.is_empty() {
                partition_by.push_str(", ");
            }
            partition_by.push_str(&quote_join_identifier(name));
        }
        let mut ordering = String::new();
        for (name, ascending) in order {
            if !ordering.is_empty() {
                ordering.push_str(", ");
            }
            let direction = if *ascending { "ASC" } else { "DESC" };
            let nulls = if *ascending { "FIRST" } else { "LAST" };
            ordering.push_str(&quote_join_identifier(name));
            ordering.push(' ');
            ordering.push_str(direction);
            ordering.push_str(" NULLS ");
            ordering.push_str(nulls);
        }
        let function = if dense { "DENSE_RANK" } else { "RANK" };
        let alias = quote_join_identifier(output_column);
        self.sql_read_only(&format!(
            "SELECT *, {function}() OVER (PARTITION BY {partition_by} ORDER BY {ordering}) AS {alias} FROM {source}"
        ))
        .await
    }
}

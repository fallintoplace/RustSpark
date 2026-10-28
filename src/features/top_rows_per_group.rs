use crate::{RustSparkSession, quote_catalog_table, quote_join_identifier};
use datafusion::{
    error::{DataFusionError, Result},
    prelude::DataFrame,
};

impl RustSparkSession {
    /// Keep at most `limit` rows per partition using a SQL window plan.
    ///
    /// A true sort direction is ascending with NULLs first. Add a unique
    /// tie-breaker to select stable rows when sort values are equal. The
    /// returned rows have no guaranteed output order.
    pub async fn top_rows_per_group(
        &self,
        table: &str,
        partitions: &[&str],
        order: &[(&str, bool)],
        limit: usize,
    ) -> Result<DataFrame> {
        if partitions.is_empty() || order.is_empty() {
            return Err(DataFusionError::Plan(
                "top_rows_per_group requires partition and sort keys".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, partitions)?;
        let keys = order.iter().map(|(name, _)| *name).collect::<Vec<_>>();
        Self::validate_selected_columns(&dataframe, &keys)?;

        let internal = "__rustspark_partition_row_number";
        if dataframe
            .schema()
            .fields()
            .iter()
            .any(|field| field.name().eq_ignore_ascii_case(internal))
        {
            return Err(DataFusionError::Plan(
                "top_rows_per_group internal column name conflicts with the input".into(),
            ));
        }
        if limit == 0 {
            return dataframe.limit(0, Some(0));
        }
        let source = quote_catalog_table(table, "top_rows_per_group requires a table path")?;
        let group_by = partitions
            .iter()
            .map(|name| quote_join_identifier(name))
            .collect::<Vec<_>>()
            .join(", ");
        let ordering = order
            .iter()
            .map(|(name, ascending)| {
                let direction = if *ascending { "ASC" } else { "DESC" };
                let nulls = if *ascending { "FIRST" } else { "LAST" };
                format!("{} {direction} NULLS {nulls}", quote_join_identifier(name))
            })
            .collect::<Vec<_>>()
            .join(", ");
        let projection = dataframe
            .schema()
            .fields()
            .iter()
            .map(|field| quote_join_identifier(field.name()))
            .collect::<Vec<_>>()
            .join(", ");
        let rank = quote_join_identifier(internal);
        self.sql_read_only(&format!(
            "SELECT {projection} FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {group_by} ORDER BY {ordering}) AS {rank} FROM {source}) AS ranked WHERE {rank} <= {limit}"
        ))
        .await
    }
}

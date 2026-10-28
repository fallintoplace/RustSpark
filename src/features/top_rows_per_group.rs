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
        let mut projection = String::new();
        for field in dataframe.schema().fields() {
            if !projection.is_empty() {
                projection.push_str(", ");
            }
            projection.push_str(&quote_join_identifier(field.name()));
        }
        let rank = quote_join_identifier(internal);
        self.sql_read_only(&format!(
            "SELECT {projection} FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {group_by} ORDER BY {ordering}) AS {rank} FROM {source}) AS ranked WHERE {rank} <= {limit}"
        ))
        .await
    }
}

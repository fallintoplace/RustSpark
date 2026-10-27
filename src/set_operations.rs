use datafusion::{
    error::{DataFusionError, Result},
    prelude::DataFrame,
};

use crate::RustSparkSession;

impl RustSparkSession {
    async fn set_operation(
        &self,
        left: &str,
        right: &str,
        operator: &'static str,
    ) -> Result<DataFrame> {
        let left =
            super::quote_catalog_table(left, "Set operations require a non-empty table path")?;
        let right =
            super::quote_catalog_table(right, "Set operations require a non-empty table path")?;
        self.sql_read_only(&format!(
            "SELECT * FROM {left} {operator} SELECT * FROM {right}"
        ))
        .await
    }

    /// Keep each distinct row that occurs in both registered tables.
    ///
    /// Columns are matched by position and must have compatible types.
    /// The returned DataFrame is lazy; NULLs follow SQL set semantics.
    pub async fn intersect_tables(&self, left: &str, right: &str) -> Result<DataFrame> {
        self.set_operation(left, right, "INTERSECT").await
    }

    /// Keep distinct rows in the left table but not the right table.
    pub async fn except_tables(&self, left: &str, right: &str) -> Result<DataFrame> {
        self.set_operation(left, right, "EXCEPT").await
    }

    // DataFusion currently retains too many left rows for INTERSECT ALL
    // with duplicate keys. Number equal rows in each input and pair copies
    // one-for-one so bag cardinalities and SQL NULL semantics stay correct.
    async fn set_operation_all(
        &self,
        left: &str,
        right: &str,
        intersection: bool,
    ) -> Result<DataFrame> {
        let left_path =
            super::quote_catalog_table(left, "Set operations require a non-empty table path")?;
        let right_path =
            super::quote_catalog_table(right, "Set operations require a non-empty table path")?;
        let left_frame = self.table(left).await?;
        let right_frame = self.table(right).await?;
        let left_columns = left_frame
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_owned())
            .collect::<Vec<_>>();
        let right_columns = right_frame
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_owned())
            .collect::<Vec<_>>();
        if left_columns.is_empty() || left_columns.len() != right_columns.len() {
            return Err(DataFusionError::Plan(
                "Set operations require the same non-empty column count".into(),
            ));
        }

        let quote = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
        let mut index_name = "__rustspark_multiset_row".to_owned();
        while left_columns
            .iter()
            .chain(&right_columns)
            .any(|name| name.eq_ignore_ascii_case(&index_name))
        {
            index_name.push('_');
        }
        let index = quote(&index_name);
        let left_partition = left_columns
            .iter()
            .map(|name| quote(name))
            .collect::<Vec<_>>()
            .join(", ");
        let right_partition = right_columns
            .iter()
            .map(|name| quote(name))
            .collect::<Vec<_>>()
            .join(", ");
        let projection = left_columns
            .iter()
            .map(|name| format!("l.{}", quote(name)))
            .collect::<Vec<_>>()
            .join(", ");
        let mut conditions = left_columns
            .iter()
            .zip(&right_columns)
            .map(|(left, right)| {
                format!(
                    "(l.{} IS NOT DISTINCT FROM r.{})",
                    quote(left),
                    quote(right)
                )
            })
            .collect::<Vec<_>>();
        conditions.push(format!("l.{index} = r.{index}"));
        let (join, filter) = if intersection {
            ("INNER JOIN", "")
        } else {
            ("LEFT JOIN", " WHERE r.__PLACEHOLDER__ IS NULL")
        };
        let filter = filter.replace("__PLACEHOLDER__", &index);
        let sql = format!(
            "SELECT {projection} FROM \
             (SELECT *, ROW_NUMBER() OVER (PARTITION BY {left_partition}) AS {index} \
              FROM {left_path}) AS l \
             {join} \
             (SELECT *, ROW_NUMBER() OVER (PARTITION BY {right_partition}) AS {index} \
              FROM {right_path}) AS r \
             ON {}{filter}",
            conditions.join(" AND "),
        );
        self.sql_read_only(&sql).await
    }

    /// Keep duplicate rows common to both tables. A row that occurs m times
    /// on the left and n times on the right occurs min(m, n) times.
    pub async fn intersect_all_tables(&self, left: &str, right: &str) -> Result<DataFrame> {
        self.set_operation_all(left, right, true).await
    }

    /// Subtract row counts, keeping max(left_count - right_count, 0) copies.
    pub async fn except_all_tables(&self, left: &str, right: &str) -> Result<DataFrame> {
        self.set_operation_all(left, right, false).await
    }

    /// Return distinct rows belonging to exactly one of the registered tables.
    ///
    /// This combines both directed SQL EXCEPT results, with SQL NULL equality.
    /// The schemas must be compatible by column position.
    pub async fn symmetric_difference_tables(&self, left: &str, right: &str) -> Result<DataFrame> {
        let left =
            super::quote_catalog_table(left, "Set operations require a non-empty table path")?;
        let right =
            super::quote_catalog_table(right, "Set operations require a non-empty table path")?;
        self.sql_read_only(&format!(
            "SELECT * FROM (SELECT * FROM {left} EXCEPT SELECT * FROM {right}) AS left_only \
             UNION ALL \
             SELECT * FROM (SELECT * FROM {right} EXCEPT SELECT * FROM {left}) AS right_only"
        ))
        .await
    }
}

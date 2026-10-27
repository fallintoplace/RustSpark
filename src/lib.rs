use std::sync::Arc;

use datafusion::{
    arrow::record_batch::RecordBatch,
    error::{DataFusionError, Result},
    execution::runtime_env::RuntimeEnv,
    prelude::{DataFrame, SessionConfig, SessionContext},
};

mod export;
mod set_operations;
mod sources;

pub use export::SqlExportFormat;

fn quote_join_identifier(name: &str) -> String {
    // Avoid a replacement string followed by a second formatting allocation.
    let mut quoted = String::with_capacity(name.len() + 2);
    quoted.push('"');
    for character in name.chars() {
        if character == '"' {
            quoted.push('"');
        }
        quoted.push(character);
    }
    quoted.push('"');
    quoted
}

#[cfg(test)]
mod join_identifier_tests {
    use super::quote_join_identifier;

    #[test]
    fn quotes_names_without_intermediate_strings() {
        assert_eq!(quote_join_identifier("col"), "\"col\"");
        assert_eq!(quote_join_identifier("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_join_identifier("é名\""), "\"é名\"\"\"");
        assert_eq!(quote_join_identifier(""), "\"\"");
    }
}

fn quote_catalog_table(path: &str, invalid_name_message: &str) -> Result<String> {
    // Avoid a Vec and an owned String for each path component.
    let mut quoted = String::with_capacity(path.len() + 8);
    for (index, part) in path.split('.').enumerate() {
        if part.is_empty() || part.trim() != part {
            return Err(DataFusionError::Plan(invalid_name_message.into()));
        }
        if index != 0 {
            quoted.push('.');
        }
        quoted.push('"');
        for character in part.chars() {
            if character == '"' {
                quoted.push('"');
            }
            quoted.push(character);
        }
        quoted.push('"');
    }
    Ok(quoted)
}

fn quote_join_table(path: &str) -> Result<String> {
    quote_catalog_table(path, "Join table name must be a non-empty catalog path")
}

/// Null filtering policy for Spark-style dropna operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NullDropMode {
    /// Drop a row if any selected value is NULL.
    Any,
    /// Drop a row only if every selected value is NULL.
    All,
}

/// Which endpoints of a range should match during an interval join.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeJoinBounds {
    /// start < value < end
    Open,
    /// start <= value <= end
    Closed,
    /// start <= value < end
    LeftClosed,
    /// start < value <= end
    RightClosed,
}

pub struct RustSparkSession {
    context: SessionContext,
}

impl RustSparkSession {
    pub fn new() -> Self {
        Self::with_config(SessionConfig::new())
    }

    pub fn with_config(config: SessionConfig) -> Self {
        Self {
            context: SessionContext::new_with_config(config),
        }
    }

    /// Wrap a preconfigured DataFusion context, including an externally
    /// provided execution backend such as Apache DataFusion Ballista.
    ///
    /// RustSpark does not take ownership of any scheduler or executor process.
    pub fn from_context(context: SessionContext) -> Self {
        Self { context }
    }

    pub fn with_config_and_runtime(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        Self {
            context: SessionContext::new_with_config_rt(config, runtime),
        }
    }

    /// Return the underlying provider for cancellation-safe table replacement.
    pub async fn table_provider(
        &self,
        name: &str,
    ) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        self.context.table_provider(name).await
    }

    /// Register an existing provider, preserving its physical table behavior.
    pub fn register_table_provider(
        &self,
        name: &str,
        provider: Arc<dyn datafusion::datasource::TableProvider>,
    ) -> Result<()> {
        self.context.register_table(name, provider)?;
        Ok(())
    }

    /// Return available catalog names in stable sorted order.
    ///
    /// Empty catalogs are included.
    pub fn list_catalogs(&self) -> Vec<String> {
        let mut catalogs = self.context.catalog_names();
        catalogs.sort();
        catalogs
    }

    fn sorted_catalog_entries(&self, include_tables: bool) -> Vec<String> {
        let mut entries = Vec::new();
        for catalog_name in self.context.catalog_names() {
            if let Some(catalog) = self.context.catalog(&catalog_name) {
                for schema_name in catalog.schema_names() {
                    if include_tables {
                        if let Some(schema) = catalog.schema(&schema_name) {
                            for table_name in schema.table_names() {
                                entries.push(format!("{catalog_name}.{schema_name}.{table_name}"));
                            }
                        }
                    } else {
                        entries.push(format!("{catalog_name}.{schema_name}"));
                    }
                }
            }
        }
        entries.sort();
        entries
    }

    /// List registered tables with fully qualified catalog.schema.table names.
    ///
    /// Results are sorted for stable inspection and CLI integration.
    pub fn list_tables(&self) -> Vec<String> {
        self.sorted_catalog_entries(true)
    }

    /// List fully qualified catalog.schema names in sorted order.
    ///
    /// Empty schemas are included even when they contain no tables.
    pub fn list_schemas(&self) -> Vec<String> {
        self.sorted_catalog_entries(false)
    }

    /// List unqualified table names in a catalog schema.
    ///
    /// The result is sorted, and missing catalogs or schemas return errors.
    pub fn list_tables_in_schema(&self, catalog: &str, schema: &str) -> Result<Vec<String>> {
        let catalog_provider = self
            .context
            .catalog(catalog)
            .ok_or_else(|| DataFusionError::Plan(format!("Unknown catalog: {catalog}")))?;
        let schema_provider = catalog_provider
            .schema(schema)
            .ok_or_else(|| DataFusionError::Plan(format!("Unknown schema: {catalog}.{schema}")))?;
        let mut tables = schema_provider.table_names();
        tables.sort();
        Ok(tables)
    }

    /// List schema names within a catalog, sorted for stable inspection.
    ///
    /// Unknown catalogs return an error instead of an empty list.
    pub fn list_schemas_in_catalog(&self, catalog_name: &str) -> Result<Vec<String>> {
        let catalog = self
            .context
            .catalog(catalog_name)
            .ok_or_else(|| DataFusionError::Plan(format!("Unknown catalog: {catalog_name}")))?;
        let mut schemas = catalog.schema_names();
        schemas.sort();
        Ok(schemas)
    }

    /// Create a lazy DataFrame from in-memory Arrow record batches.
    pub fn read_batches(
        &self,
        batches: impl IntoIterator<Item = RecordBatch>,
    ) -> Result<DataFrame> {
        self.context.read_batches(batches)
    }

    /// Execute a SQL query as a stream of Arrow record batches.
    ///
    /// Clients can process batches incrementally without collecting all rows.
    pub async fn sql_stream(
        &self,
        query: &str,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        self.sql(query).await?.execute_stream().await
    }

    /// Stream a SQL query with typed positional or named parameters.
    ///
    /// Values are bound by DataFusion before execution. Results remain
    /// incremental rather than being collected in memory.
    pub async fn sql_stream_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        self.sql_with_params(query, params)
            .await?
            .execute_stream()
            .await
    }

    /// Register in-memory Arrow batches for SQL queries.
    pub fn register_batches(
        &self,
        table: &str,
        batches: impl IntoIterator<Item = RecordBatch>,
    ) -> Result<()> {
        self.register_dataframe(table, self.read_batches(batches)?)
    }

    /// Stream a registered table as Arrow record batches.
    ///
    /// Rows are consumed incrementally without collecting the full table.
    pub async fn table_stream(
        &self,
        table: &str,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        self.table(table).await?.execute_stream().await
    }

    async fn register_view_dataframe(
        &self,
        name: &str,
        dataframe: DataFrame,
        materialized: bool,
    ) -> Result<()> {
        let dataframe = if materialized {
            dataframe.cache().await?
        } else {
            dataframe
        };
        self.register_dataframe(name, dataframe)
    }

    /// Register a SQL query as a lazy view in this session.
    ///
    /// The query is planned now but its rows are read when the view is queried.
    pub async fn register_sql_view(&self, name: &str, query: &str) -> Result<()> {
        self.register_view_dataframe(name, self.sql_read_only(query).await?, false)
            .await
    }

    pub async fn register_materialized_sql_view(&self, name: &str, query: &str) -> Result<()> {
        self.register_view_dataframe(name, self.sql_read_only(query).await?, true)
            .await
    }

    /// Register a lazy SQL view with typed positional or named parameters.
    ///
    /// Parameter values are bound when the view is planned, not interpolated
    /// into the SQL string.
    pub async fn register_sql_view_with_params(
        &self,
        name: &str,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<()> {
        self.register_view_dataframe(
            name,
            self.sql_read_only_with_params(query, params).await?,
            false,
        )
        .await
    }

    /// Materialize a parameterized SQL query once and register its cached result.
    pub async fn register_materialized_sql_view_with_params(
        &self,
        name: &str,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<()> {
        self.register_view_dataframe(
            name,
            self.sql_read_only_with_params(query, params).await?,
            true,
        )
        .await
    }

    /// Materialize read-only SQL results into an independent cached DataFrame.
    ///
    /// The snapshot stays available after the source table is deregistered.
    pub async fn sql_snapshot(&self, query: &str) -> Result<DataFrame> {
        self.sql_read_only(query).await?.cache().await
    }

    /// Materialize parameterized, read-only SQL with typed bindings.
    pub async fn sql_snapshot_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<DataFrame> {
        self.sql_read_only_with_params(query, params)
            .await?
            .cache()
            .await
    }

    /// Join two registered tables by their shared column names (SQL USING).
    ///
    /// Each join key appears once in the output, unlike an ON join. FULL OUTER
    /// JOIN coalesces the key from whichever side supplies the row. Other
    /// non-key columns are left unchanged, even when their names collide.
    /// The join is lazy and its key columns appear first in the output.
    pub async fn join_tables_using(
        &self,
        left: &str,
        right: &str,
        join_type: datafusion::logical_expr::JoinType,
        columns: &[&str],
    ) -> Result<DataFrame> {
        use datafusion::logical_expr::JoinType;
        use std::collections::HashSet;

        if columns.is_empty() || columns.iter().any(|column| column.trim().is_empty()) {
            return Err(DataFusionError::Plan(
                "join_tables_using requires one or more non-empty column names".into(),
            ));
        }
        let mut seen = HashSet::new();
        for column in columns {
            if !seen.insert(column.to_ascii_lowercase()) {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate join key: {column}"
                )));
            }
        }
        let operator = match join_type {
            JoinType::Inner => "INNER",
            JoinType::Left => "LEFT",
            JoinType::Right => "RIGHT",
            JoinType::Full => "FULL OUTER",
            _ => {
                return Err(DataFusionError::Plan(
                    "join_tables_using supports inner and outer joins only".into(),
                ));
            }
        };

        // The two read-only table lookups are independent. Resolve them
        // together, but keep the left/right schema order in the final plan.
        let (left_frame, right_frame) = tokio::try_join!(self.table(left), self.table(right))?;
        for (table, frame) in [(left, &left_frame), (right, &right_frame)] {
            let field_names: HashSet<&str> = frame
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect();
            for column in columns {
                if !field_names.contains(*column) {
                    return Err(DataFusionError::Plan(format!(
                        "Join column {column} does not exist in table {table}"
                    )));
                }
            }
        }

        // Escape identifiers rather than interpolating arbitrary SQL text.
        let left_alias = "\"__rustspark_left\"";
        let right_alias = "\"__rustspark_right\"";

        // DataFusion 55's SELECT * FULL JOIN USING may return a NULL join key
        // for a row preserved only on the left. An explicit projection avoids
        // that behavior and gives all join types the same output column order.
        use std::fmt::Write as _;
        let projected_fields =
            left_frame.schema().fields().len() + right_frame.schema().fields().len();
        let mut projection = String::with_capacity(projected_fields * 32);
        let mut predicates = String::with_capacity(columns.len() * 64);
        for column in columns {
            let key = quote_join_identifier(column);
            if !projection.is_empty() {
                projection.push_str(", ");
            }
            write!(
                &mut projection,
                "COALESCE({left_alias}.{key}, {right_alias}.{key}) AS {key}"
            )
            .expect("writing to a String cannot fail");
            if !predicates.is_empty() {
                predicates.push_str(" AND ");
            }
            write!(&mut predicates, "{left_alias}.{key} = {right_alias}.{key}")
                .expect("writing to a String cannot fail");
        }
        for (frame, alias) in [(&left_frame, left_alias), (&right_frame, right_alias)] {
            for field in frame.schema().fields() {
                if !seen.contains(&field.name().to_ascii_lowercase()) {
                    let name = quote_join_identifier(field.name());
                    if !projection.is_empty() {
                        projection.push_str(", ");
                    }
                    write!(&mut projection, "{alias}.{name} AS {name}")
                        .expect("writing to a String cannot fail");
                }
            }
        }
        let query = format!(
            "SELECT {projection} FROM {} AS {left_alias} {operator} JOIN {} AS {right_alias} ON {predicates}",
            quote_join_table(left)?,
            quote_join_table(right)?,
        );
        self.sql(&query).await
    }

    /// Join on every column name that both registered tables share.
    ///
    /// Shared columns are selected in left-table schema order and appear once
    /// in the output, as in a USING join. Columns are matched by exact name.
    /// Unlike SQL NATURAL JOIN, this rejects a pair with no common columns
    /// rather than producing an accidental Cartesian product.
    pub async fn join_tables_on_common_columns(
        &self,
        left: &str,
        right: &str,
        join_type: datafusion::logical_expr::JoinType,
    ) -> Result<DataFrame> {
        use std::collections::HashSet;

        let (left_frame, right_frame) = tokio::try_join!(self.table(left), self.table(right))?;
        let right_columns: HashSet<&str> = right_frame
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        let columns: Vec<&str> = left_frame
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .filter(|column| right_columns.contains(column))
            .collect();
        if columns.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "Tables {left} and {right} have no common join columns"
            )));
        }
        self.join_tables_using(left, right, join_type, &columns)
            .await
    }

    pub fn register_dataframe(&self, table: &str, dataframe: DataFrame) -> Result<()> {
        if dataframe.task_ctx().session_id() != self.context.session_id() {
            return Err(DataFusionError::Plan(
                "DataFrame belongs to a different RustSpark session".into(),
            ));
        }

        self.context.register_table(table, dataframe.into_view())?;
        Ok(())
    }

    fn describe_columns(
        dataframe: &DataFrame,
    ) -> Vec<(String, datafusion::arrow::datatypes::DataType, bool)> {
        dataframe
            .schema()
            .fields()
            .iter()
            .map(|field| {
                (
                    field.name().to_owned(),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect()
    }

    /// Return column names, data types and nullability without scanning rows.
    pub async fn describe_table(
        &self,
        name: &str,
    ) -> Result<Vec<(String, datafusion::arrow::datatypes::DataType, bool)>> {
        let dataframe = self.table(name).await?;
        Ok(Self::describe_columns(&dataframe))
    }

    /// Read a registered table's column names in schema order.
    ///
    /// This inspects metadata without executing the table's rows.
    pub async fn column_names(&self, table: &str) -> Result<Vec<String>> {
        let dataframe = self.table(table).await?;
        Ok(dataframe
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_owned())
            .collect())
    }

    /// Count NULL values in selected table columns without collecting rows.
    ///
    /// An empty column list includes all columns. The result is a lazy
    /// one-row DataFrame with one Int64 count per selected column.
    pub async fn null_counts(&self, table: &str, columns: &[&str]) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        let selected: Vec<&str> = if columns.is_empty() {
            dataframe
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect()
        } else {
            columns.to_vec()
        };
        if selected.is_empty() {
            return Err(DataFusionError::Plan(
                "null_counts requires at least one column".into(),
            ));
        }
        Self::validate_selected_columns(&dataframe, &selected)?;

        let expressions = selected
            .iter()
            .map(|name| {
                let quoted = quote_join_identifier(name);
                format!("COUNT(*) - COUNT({quoted}) AS {quoted}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        let source = quote_catalog_table(table, "null_counts requires a table name")?;
        self.sql(&format!("SELECT {expressions} FROM {source}"))
            .await
    }

    /// Inspect the names, types and nullability of a read-only SQL query.
    ///
    /// Query results are not executed or collected.
    pub async fn describe_sql(
        &self,
        query: &str,
    ) -> Result<Vec<(String, datafusion::arrow::datatypes::DataType, bool)>> {
        let dataframe = self.sql_read_only(query).await?;
        Ok(Self::describe_columns(&dataframe))
    }

    /// Inspect output columns from a parameterized, read-only SQL query.
    ///
    /// This plans the query without scanning input rows.
    pub async fn describe_sql_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<Vec<(String, datafusion::arrow::datatypes::DataType, bool)>> {
        let dataframe = self.sql_read_only_with_params(query, params).await?;
        Ok(Self::describe_columns(&dataframe))
    }

    fn validate_named_columns(
        dataframe: &DataFrame,
        columns: &[&str],
        operation: &str,
    ) -> Result<()> {
        use std::collections::HashSet;

        // One-name lookups are cheap as a scan. Wide projections benefit from
        // indexing the schema once rather than rescanning it for every field.
        let fields = dataframe.schema().fields();
        let indexed = (columns.len() > 8).then(|| {
            fields
                .iter()
                .map(|field| field.name().as_str())
                .collect::<HashSet<_>>()
        });
        let mut seen = HashSet::new();
        for &name in columns {
            if !seen.insert(name.to_ascii_lowercase()) {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate {operation} column: {name}"
                )));
            }
            let present = match &indexed {
                Some(indexed) => indexed.contains(name),
                None => fields.iter().any(|field| field.name().as_str() == name),
            };
            if !present {
                return Err(DataFusionError::Plan(format!(
                    "Unknown {operation} column: {name}"
                )));
            }
        }
        Ok(())
    }

    fn validate_selected_columns(dataframe: &DataFrame, columns: &[&str]) -> Result<()> {
        Self::validate_named_columns(dataframe, columns, "selected")
    }

    /// Select and reorder columns from a registered table without reading its rows.
    ///
    /// Names must exist and cannot repeat (case-insensitively). At least one
    /// column must be selected. The source table remains unchanged.
    pub async fn select_columns(&self, table: &str, columns: &[&str]) -> Result<DataFrame> {
        if columns.is_empty() {
            return Err(DataFusionError::Plan(
                "select_columns requires at least one column".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, columns)?;
        dataframe.select_columns(columns)
    }

    /// Return the unique values from a registered column as a lazy DataFrame.
    ///
    /// SQL DISTINCT preserves one NULL value; result order is not guaranteed.
    pub async fn distinct_column_values(&self, table: &str, column: &str) -> Result<DataFrame> {
        self.select_columns(table, &[column]).await?.distinct()
    }

    /// Select distinct combinations of the named columns from a table.
    ///
    /// Column order follows the caller's list. The operation is lazy and
    /// does not alter the registered table.
    pub async fn distinct_columns(&self, table: &str, columns: &[&str]) -> Result<DataFrame> {
        if columns.is_empty() {
            return Err(DataFusionError::Plan(
                "distinct_columns requires at least one column".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, columns)?;
        dataframe.select_columns(columns)?.distinct()
    }

    /// Project typed DataFusion expressions from a registered table.
    ///
    /// Computed expressions and aliases are planned lazily. At least one
    /// expression is required and the source table remains unchanged.
    pub async fn select_expressions(
        &self,
        table: &str,
        expressions: &[datafusion::logical_expr::Expr],
    ) -> Result<DataFrame> {
        if expressions.is_empty() {
            return Err(DataFusionError::Plan(
                "select_expressions requires at least one expression".into(),
            ));
        }
        self.table(table).await?.select(expressions.to_vec())
    }

    fn validate_dropna_columns(dataframe: &DataFrame, selected: &[&str]) -> Result<()> {
        Self::validate_named_columns(dataframe, selected, "dropna")
    }

    /// Drop rows containing NULL values, like Spark DataFrame.na.drop.
    ///
    /// An empty column list checks every column. Any drops rows with at
    /// least one NULL; All drops only rows whose selected columns are all NULL.
    /// The returned DataFrame is lazy and leaves the source table unchanged.
    pub async fn drop_null_rows(
        &self,
        table: &str,
        columns: &[&str],
        mode: NullDropMode,
    ) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr};

        let dataframe = self.table(table).await?;
        let selected: Vec<&str> = if columns.is_empty() {
            dataframe
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect()
        } else {
            columns.to_vec()
        };
        if selected.is_empty() {
            return Ok(dataframe);
        }

        Self::validate_dropna_columns(&dataframe, &selected)?;

        let mut checks = selected
            .iter()
            .map(|name| Expr::Column(Column::from_name(*name)).is_not_null());
        let first = checks.next().expect("non-empty selected columns");
        let predicate = checks.fold(first, |acc, check| match mode {
            NullDropMode::Any => acc.and(check),
            NullDropMode::All => acc.or(check),
        });
        dataframe.filter(predicate)
    }

    /// Keep rows containing NULLs in selected columns.
    ///
    /// An empty column list examines every column. Any matches a row when
    /// at least one value is NULL; All requires every selected value to be NULL.
    /// The result is lazy and leaves the source table unchanged.
    pub async fn filter_null_rows(
        &self,
        table: &str,
        columns: &[&str],
        mode: NullDropMode,
    ) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr};

        let dataframe = self.table(table).await?;
        let selected: Vec<&str> = if columns.is_empty() {
            dataframe
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect()
        } else {
            columns.to_vec()
        };
        if selected.is_empty() {
            return Ok(dataframe);
        }
        Self::validate_dropna_columns(&dataframe, &selected)?;

        let mut checks = selected
            .iter()
            .map(|name| Expr::Column(Column::from_name(*name)).is_null());
        let first = checks.next().expect("non-empty selected columns");
        let predicate = checks.fold(first, |acc, check| match mode {
            NullDropMode::Any => acc.or(check),
            NullDropMode::All => acc.and(check),
        });
        dataframe.filter(predicate)
    }

    /// Filter a registered table with a typed DataFusion expression.
    ///
    /// SQL NULL predicates do not keep rows. The operation is lazy, and the
    /// source table is not modified. Use expressions instead of SQL string
    /// concatenation when filtering on programmatic values.
    pub async fn filter_rows(
        &self,
        table: &str,
        predicate: datafusion::logical_expr::Expr,
    ) -> Result<DataFrame> {
        self.table(table).await?.filter(predicate)
    }

    /// Keep rows whose selected column is within an inclusive typed range.
    ///
    /// NULL rows are excluded by SQL comparison semantics. Invalid columns,
    /// incompatible types, and NULL bounds are rejected during planning.
    pub async fn filter_rows_between(
        &self,
        table: &str,
        column: &str,
        lower: datafusion::common::ScalarValue,
        upper: datafusion::common::ScalarValue,
    ) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr, prelude::lit};

        if lower.is_null() || upper.is_null() {
            return Err(DataFusionError::Plan(
                "filter_rows_between requires non-NULL bounds".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        let key = Expr::Column(Column::from_name(column));
        dataframe.filter(key.clone().gt_eq(lit(lower)).and(key.lt_eq(lit(upper))))
    }

    /// Filter rows against typed values without SQL string interpolation.
    ///
    /// A NULL entry follows SQL IN semantics. An empty set matches no rows.
    /// The source table is left unchanged.
    pub async fn filter_rows_in_values(
        &self,
        table: &str,
        column: &str,
        values: &[datafusion::common::ScalarValue],
    ) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        if values.is_empty() {
            return dataframe.filter(datafusion::prelude::lit(false));
        }
        use datafusion::{common::Column, logical_expr::Expr};
        let list = values
            .iter()
            .cloned()
            .map(datafusion::prelude::lit)
            .collect();
        dataframe.filter(Expr::Column(Column::from_name(column)).in_list(list, false))
    }

    /// Register a scalar user-defined function for SQL queries in this session.
    pub fn register_udf(&self, udf: datafusion::logical_expr::ScalarUDF) {
        self.context.register_udf(udf);
    }

    /// Register an aggregate user-defined function for SQL queries.
    pub fn register_udaf(&self, udf: datafusion::logical_expr::AggregateUDF) {
        self.context.register_udaf(udf);
    }

    /// Register a window user-defined function for SQL queries.
    pub fn register_udwf(&self, udf: datafusion::logical_expr::WindowUDF) {
        self.context.register_udwf(udf);
    }

    /// Unregister a table or view, returning whether it existed.
    pub fn deregister_table(&self, name: &str) -> Result<bool> {
        Ok(self.context.deregister_table(name)?.is_some())
    }

    /// Return whether a table or view is registered in this session.
    pub fn table_exists(&self, name: &str) -> Result<bool> {
        self.context.table_exist(name)
    }

    /// Count rows in a registered table.
    ///
    /// This runs a count aggregate without collecting the table's rows.
    pub async fn table_row_count(&self, table: &str) -> Result<usize> {
        self.table(table).await?.count().await
    }

    /// Rename a registered table without copying its data.
    ///
    /// An existing destination is never overwritten. The new registration
    /// is rolled back if removing the original fails.
    pub async fn rename_table(&self, source: &str, destination: &str) -> Result<()> {
        if source == destination || self.context.table_exist(destination)? {
            return Err(DataFusionError::Plan(format!(
                "Destination table already exists: {destination}"
            )));
        }
        let provider = self.context.table_provider(source).await?;
        self.context.register_table(destination, provider)?;
        let result = match self.context.deregister_table(source) {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(DataFusionError::Plan(format!(
                "Source table disappeared while renaming: {source}"
            ))),
            Err(error) => Err(error),
        };
        if result.is_err() {
            let _ = self.context.deregister_table(destination);
        }
        result
    }

    fn renamed_column_projection(
        dataframe: &DataFrame,
        renames: &[(&str, &str)],
    ) -> Result<Vec<datafusion::logical_expr::Expr>> {
        use datafusion::{common::Column, logical_expr::Expr};
        use std::collections::{HashMap, HashSet};

        let mut mappings = HashMap::new();
        for &(source, target) in renames {
            if source.trim().is_empty() || target.trim().is_empty() {
                return Err(DataFusionError::Plan(
                    "Column rename requires non-empty names".into(),
                ));
            }
            if mappings.insert(source, target).is_some() {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate column rename source: {source}"
                )));
            }
        }
        let available: HashSet<&str> = dataframe
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        for source in mappings.keys() {
            if !available.contains(*source) {
                return Err(DataFusionError::Plan(format!(
                    "Unknown column rename source: {source}"
                )));
            }
        }

        let mut names = HashSet::new();
        let mut projection = Vec::new();
        for field in dataframe.schema().fields() {
            let source = field.name().as_str();
            let target = mappings.get(source).copied().unwrap_or(source);
            if !names.insert(target.to_ascii_lowercase()) {
                return Err(DataFusionError::Plan(format!(
                    "Column rename produces duplicate name: {target}"
                )));
            }
            let column = Expr::Column(Column::from_name(source));
            projection.push(if target == source {
                column
            } else {
                column.alias(target)
            });
        }
        Ok(projection)
    }

    /// Rename several columns atomically, like Spark withColumnsRenamed.
    ///
    /// All names refer to the original schema, so swaps such as a -> b and
    /// b -> a work without intermediate collisions. This does not change the
    /// registered table or collect its rows.
    pub async fn rename_columns(&self, table: &str, renames: &[(&str, &str)]) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        if renames.is_empty() {
            return Ok(dataframe);
        }
        let projection = Self::renamed_column_projection(&dataframe, renames)?;
        dataframe.select(projection)
    }

    /// Drop columns by name, like Spark DataFrame.drop.
    ///
    /// Unknown or repeated names are ignored. At least one column must remain,
    /// since DataFusion cannot represent an empty projection reliably.
    /// The returned DataFrame is lazy and does not change the source.
    pub async fn drop_columns(&self, table: &str, columns: &[&str]) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr};
        use std::collections::HashSet;

        let dataframe = self.table(table).await?;
        if columns.is_empty() {
            return Ok(dataframe);
        }
        let removed: HashSet<&str> = columns.iter().copied().collect();
        let remaining: Vec<_> = dataframe
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .filter(|name| !removed.contains(name))
            .map(|name| Expr::Column(Column::from_name(name)))
            .collect();
        if remaining.is_empty() {
            return Err(DataFusionError::Plan(
                "drop_columns would remove every column".into(),
            ));
        }
        dataframe.select(remaining)
    }

    /// Materialize a registered table in memory for repeated queries.
    ///
    /// This executes the table's plan once and keeps its contents in memory.
    /// Use only when the result fits comfortably within the available RAM.
    pub async fn cache_table(&self, name: &str) -> Result<()> {
        let cached = self.table(name).await?.cache().await?;
        let previous = self.context.deregister_table(name)?;
        let result = self.register_dataframe(name, cached);
        if result.is_err()
            && let Some(previous) = previous
        {
            self.context.register_table(name, previous)?;
        }
        result
    }

    /// Fill NULLs in several columns, like Spark DataFrame.na.fill(map).
    ///
    /// Values are typed Arrow scalars. Each replacement is applied to its
    /// selected column only, preserving other values and column order.
    /// DataFusion ignores replacements incompatible with a column type.
    pub async fn fill_nulls(
        &self,
        table: &str,
        replacements: &[(&str, datafusion::common::ScalarValue)],
    ) -> Result<DataFrame> {
        use std::collections::HashSet;

        let mut dataframe = self.table(table).await?;
        let columns: HashSet<_> = dataframe
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_owned())
            .collect();
        let mut seen = HashSet::new();
        let mut grouped = Vec::<(&datafusion::common::ScalarValue, Vec<&str>)>::new();
        // Wide fillna calls used a linear search of prior groups for every
        // column. Index the typed scalar values while retaining group order.
        let mut group_indices: std::collections::HashMap<&datafusion::common::ScalarValue, usize> =
            std::collections::HashMap::new();
        for (name, value) in replacements {
            if !seen.insert(name.to_ascii_lowercase()) {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate fillna column: {name}"
                )));
            }
            if !columns.contains(*name) {
                return Err(DataFusionError::Plan(format!(
                    "Unknown fillna column: {name}"
                )));
            }
            if value.is_null() {
                return Err(DataFusionError::Plan(format!(
                    "fillna replacement cannot be NULL: {name}"
                )));
            }
            // The same typed value can fill several columns in one projection.
            if let Some(&index) = group_indices.get(value) {
                grouped[index].1.push(*name);
            } else {
                group_indices.insert(value, grouped.len());
                grouped.push((value, vec![*name]));
            }
        }
        for (value, names) in grouped {
            dataframe = dataframe.fill_null(value, &names)?;
        }
        Ok(dataframe)
    }

    fn computed_column_projection(
        dataframe: &DataFrame,
        columns: &[(&str, datafusion::logical_expr::Expr)],
    ) -> Result<Vec<datafusion::logical_expr::Expr>> {
        use datafusion::{common::Column, logical_expr::Expr};
        use std::collections::{HashMap, HashSet};

        let mut updates = HashMap::new();
        let mut seen = HashSet::new();
        for (name, expr) in columns {
            if name.trim().is_empty() || name.trim() != *name {
                return Err(DataFusionError::Plan(
                    "with_columns requires non-empty, unpadded names".into(),
                ));
            }
            if !seen.insert(name.to_ascii_lowercase()) {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate withColumns name: {name}"
                )));
            }
            updates.insert(*name, (*expr).clone());
        }

        let mut projection = Vec::new();
        for field in dataframe.schema().fields() {
            let name = field.name().as_str();
            let expression = match updates.remove(name) {
                Some(expr) => expr.alias(name),
                None => Expr::Column(Column::from_name(name)),
            };
            projection.push(expression);
        }
        for (name, _) in columns {
            if let Some(expr) = updates.remove(*name) {
                projection.push(expr.alias(*name));
            }
        }
        Ok(projection)
    }

    /// Drop rows with fewer than required non-NULL values in selected columns.
    ///
    /// An empty column slice inspects all columns. A zero threshold keeps
    /// every row. The predicate is planned lazily, like Spark dropna(thresh).
    pub async fn drop_null_rows_with_threshold(
        &self,
        table: &str,
        columns: &[&str],
        required: usize,
    ) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        let selected: Vec<&str> = if columns.is_empty() {
            dataframe
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect()
        } else {
            columns.to_vec()
        };
        Self::validate_dropna_columns(&dataframe, &selected)?;
        if required > selected.len() {
            return Err(DataFusionError::Plan(
                "drop_null_rows_with_threshold exceeds selected column count".into(),
            ));
        }
        if required == 0 {
            return Ok(dataframe);
        }
        let non_nulls = selected
            .iter()
            .map(|name| {
                format!(
                    "(CASE WHEN {} IS NOT NULL THEN 1 ELSE 0 END)",
                    quote_join_identifier(name)
                )
            })
            .collect::<Vec<_>>()
            .join(" + ");
        let source =
            quote_catalog_table(table, "drop_null_rows_with_threshold requires a table path")?;
        self.sql_read_only(&format!(
            "SELECT * FROM {source} WHERE ({non_nulls}) >= {required}"
        ))
        .await
    }

    /// Add or replace several columns atomically, like Spark withColumns.
    ///
    /// Every expression refers to the original input schema. Existing columns
    /// keep their positions, and new columns are appended in the requested order.
    /// The operation is lazy and leaves the registered table unchanged.
    pub async fn with_columns(
        &self,
        table: &str,
        columns: &[(&str, datafusion::logical_expr::Expr)],
    ) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        if columns.is_empty() {
            return Ok(dataframe);
        }

        let projection = Self::computed_column_projection(&dataframe, columns)?;
        dataframe.select(projection)
    }

    /// Add a one-based row number column using a window function.
    ///
    /// The supplied sort order determines numbering, with NULLs first for
    /// ascending keys and last for descending keys. Add a unique tie-breaker
    /// for stable numbers. The output is lazy and the input is unchanged.
    pub async fn with_row_numbers(
        &self,
        table: &str,
        output_column: &str,
        order: &[(&str, bool)],
    ) -> Result<DataFrame> {
        if output_column.is_empty() || output_column.trim() != output_column {
            return Err(DataFusionError::Plan(
                "with_row_numbers requires an unpadded output column name".into(),
            ));
        }
        if order.is_empty() {
            return Err(DataFusionError::Plan(
                "with_row_numbers requires at least one sort key".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        if dataframe
            .schema()
            .fields()
            .iter()
            .any(|field| field.name().eq_ignore_ascii_case(output_column))
        {
            return Err(DataFusionError::Plan(format!(
                "Row number column already exists: {output_column}"
            )));
        }
        // Reuse the same key and duplicate checks as sort_rows without
        // executing the resulting plan.
        Self::sort_dataframe(dataframe, order)?;
        let ordering = order
            .iter()
            .map(|(name, ascending)| {
                let direction = if *ascending { "ASC" } else { "DESC" };
                let nulls = if *ascending { "FIRST" } else { "LAST" };
                format!("{} {direction} NULLS {nulls}", quote_join_identifier(name))
            })
            .collect::<Vec<_>>()
            .join(", ");
        let source =
            quote_catalog_table(table, "with_row_numbers requires a non-empty table path")?;
        let alias = quote_join_identifier(output_column);
        self.sql_read_only(&format!(
            "SELECT *, ROW_NUMBER() OVER (ORDER BY {ordering}) AS {alias} FROM {source}"
        ))
        .await
    }

    /// Aggregate a registered table by typed grouping and aggregate expressions.
    ///
    /// An empty group list produces a single global aggregation. At least one
    /// aggregate is required. The result is lazy and does not change the table.
    pub async fn group_rows(
        &self,
        table: &str,
        groups: &[datafusion::logical_expr::Expr],
        aggregates: &[datafusion::logical_expr::Expr],
    ) -> Result<DataFrame> {
        if aggregates.is_empty() {
            return Err(DataFusionError::Plan(
                "group_rows requires at least one aggregate".into(),
            ));
        }
        self.table(table)
            .await?
            .aggregate(groups.to_vec(), aggregates.to_vec())
    }

    pub async fn register_table_alias(&self, alias: &str, source: &str) -> Result<()> {
        let dataframe = self.table(source).await?;
        self.register_dataframe(alias, dataframe)
    }

    /// Access a registered SQL table as a lazy DataFrame.
    pub async fn table(&self, name: &str) -> Result<DataFrame> {
        self.context.table(name).await
    }

    /// Read the first row of a registered table as typed scalar values.
    ///
    /// Returns None for an empty table. Use an ordered view when row order matters.
    pub async fn table_first_row(
        &self,
        table: &str,
    ) -> Result<Option<Vec<datafusion::common::ScalarValue>>> {
        Self::extract_first_row(self.table(table).await?).await
    }

    /// Return a lazy window of rows from a registered table.
    ///
    /// A length of zero produces an empty result. Sort the source before
    /// slicing if stable row ordering is required.
    pub async fn slice_rows(&self, table: &str, offset: usize, length: usize) -> Result<DataFrame> {
        offset.checked_add(length).ok_or_else(|| {
            DataFusionError::Plan("slice_rows offset plus length exceeds usize".into())
        })?;
        self.table(table).await?.limit(offset, Some(length))
    }

    /// Return a bounded page of rows from a registered table.
    ///
    /// The plan remains lazy. Use a sorted view for deterministic page order.
    /// The limit must be positive, and offset plus limit must not overflow.
    pub async fn table_page(&self, table: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        Self::paginate(self.table(table), offset, limit).await
    }

    /// Join two tables and suffix columns whose names occur on both sides.
    ///
    /// Both copies of each join key are kept, as in an ON join. Shared names
    /// receive the given left/right suffixes; unique names stay unchanged.
    /// Rejects suffixes that would still create duplicate output names.
    pub async fn join_tables_with_suffixes(
        &self,
        left: &str,
        right: &str,
        join_type: datafusion::logical_expr::JoinType,
        keys: &[(&str, &str)],
        left_suffix: &str,
        right_suffix: &str,
    ) -> Result<DataFrame> {
        use datafusion::logical_expr::JoinType;
        use std::{collections::HashSet, fmt::Write as _};

        if keys.is_empty()
            || keys.iter().any(|(left_key, right_key)| {
                left_key.trim().is_empty() || right_key.trim().is_empty()
            })
        {
            return Err(DataFusionError::Plan(
                "join_tables_with_suffixes requires non-empty key pairs".into(),
            ));
        }
        if left_suffix.trim().is_empty() || right_suffix.trim().is_empty() {
            return Err(DataFusionError::Plan(
                "join_tables_with_suffixes requires non-empty suffixes".into(),
            ));
        }
        let operator = match join_type {
            JoinType::Inner => "INNER",
            JoinType::Left => "LEFT",
            JoinType::Right => "RIGHT",
            JoinType::Full => "FULL OUTER",
            _ => {
                return Err(DataFusionError::Plan(
                    "join_tables_with_suffixes supports inner and outer joins only".into(),
                ));
            }
        };
        let (left_frame, right_frame) = tokio::try_join!(self.table(left), self.table(right))?;
        let left_names: HashSet<_> = left_frame
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_ascii_lowercase())
            .collect();
        let right_names: HashSet<_> = right_frame
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_ascii_lowercase())
            .collect();

        let mut output_names = HashSet::new();
        let mut projection = String::new();
        let left_alias = "\"__rustspark_left\"";
        let right_alias = "\"__rustspark_right\"";
        for (frame, alias, other_names, suffix) in [
            (&left_frame, left_alias, &right_names, left_suffix),
            (&right_frame, right_alias, &left_names, right_suffix),
        ] {
            for field in frame.schema().fields() {
                let name = field.name();
                let output_name = if other_names.contains(&name.to_ascii_lowercase()) {
                    format!("{name}{suffix}")
                } else {
                    name.to_owned()
                };
                if !output_names.insert(output_name.to_ascii_lowercase()) {
                    return Err(DataFusionError::Plan(format!(
                        "Join output column name is not unique: {output_name}"
                    )));
                }
                if !projection.is_empty() {
                    projection.push_str(", ");
                }
                write!(
                    &mut projection,
                    "{alias}.{} AS {}",
                    quote_join_identifier(name),
                    quote_join_identifier(&output_name),
                )
                .expect("writing to a String cannot fail");
            }
        }
        let mut predicates = String::with_capacity(keys.len() * 64);
        for (left_key, right_key) in keys {
            if !predicates.is_empty() {
                predicates.push_str(" AND ");
            }
            write!(
                &mut predicates,
                "{left_alias}.{} = {right_alias}.{}",
                quote_join_identifier(left_key),
                quote_join_identifier(right_key),
            )
            .expect("writing to a String cannot fail");
        }

        let query = format!(
            "SELECT {projection} FROM {} AS {left_alias} {operator} JOIN {} AS {right_alias} ON {predicates}",
            quote_join_table(left)?,
            quote_join_table(right)?,
        );
        self.sql(&query).await
    }

    /// Join two registered tables on one or more pairs of equality keys.
    ///
    /// The result is a lazy DataFrame. Join types such as inner, left, right,
    /// full, semi, and anti use DataFusion's planner and execution engine.
    /// Both key lists must be non-empty; use SQL CROSS JOIN for a Cartesian
    /// product. For self joins, use SQL aliases to disambiguate columns.
    pub async fn join_tables(
        &self,
        left: &str,
        right: &str,
        join_type: datafusion::logical_expr::JoinType,
        keys: &[(&str, &str)],
    ) -> Result<DataFrame> {
        if keys.is_empty()
            || keys
                .iter()
                .any(|(l, r)| l.trim().is_empty() || r.trim().is_empty())
        {
            return Err(DataFusionError::Plan(
                "join_tables requires at least one pair of non-empty join keys".into(),
            ));
        }

        let left_keys: Vec<_> = keys.iter().map(|(left, _)| *left).collect();
        let right_keys: Vec<_> = keys.iter().map(|(_, right)| *right).collect();
        let (left_frame, right_frame) = tokio::try_join!(self.table(left), self.table(right))?;
        left_frame.join(right_frame, join_type, &left_keys, &right_keys, None)
    }

    /// Join registered tables with NULL-safe equality on key pairs.
    ///
    /// Unlike ordinary equality joins, two NULL keys match. A NULL key does
    /// not match a non-NULL key. Each side keeps its columns, like an ON join.
    /// The returned DataFrame remains lazy.
    pub async fn join_tables_null_safe(
        &self,
        left: &str,
        right: &str,
        join_type: datafusion::logical_expr::JoinType,
        keys: &[(&str, &str)],
    ) -> Result<DataFrame> {
        use datafusion::logical_expr::JoinType;
        use std::fmt::Write as _;

        if keys.is_empty()
            || keys.iter().any(|(left_key, right_key)| {
                left_key.trim().is_empty() || right_key.trim().is_empty()
            })
        {
            return Err(DataFusionError::Plan(
                "join_tables_null_safe requires one or more non-empty key pairs".into(),
            ));
        }
        let operator = match join_type {
            JoinType::Inner => "INNER",
            JoinType::Left => "LEFT",
            JoinType::Right => "RIGHT",
            JoinType::Full => "FULL OUTER",
            _ => {
                return Err(DataFusionError::Plan(
                    "join_tables_null_safe supports inner and outer joins only".into(),
                ));
            }
        };
        let mut predicates = String::with_capacity(keys.len() * 96);
        for (left_key, right_key) in keys {
            if !predicates.is_empty() {
                predicates.push_str(" AND ");
            }
            write!(
                &mut predicates,
                "\"__rustspark_left\".{} IS NOT DISTINCT FROM \"__rustspark_right\".{}",
                quote_join_identifier(left_key),
                quote_join_identifier(right_key),
            )
            .expect("writing to a String cannot fail");
        }
        let query = format!(
            "SELECT * FROM {} AS \"__rustspark_left\" {operator} JOIN {} AS \"__rustspark_right\" ON {predicates}",
            quote_join_table(left)?,
            quote_join_table(right)?,
        );
        self.sql(&query).await
    }

    async fn union_registered_tables(&self, names: &[&str], by_name: bool) -> Result<DataFrame> {
        if names.is_empty() {
            let operation = if by_name {
                "union_tables_by_name"
            } else {
                "union_tables"
            };
            return Err(DataFusionError::Plan(format!(
                "{operation} requires at least one table"
            )));
        }
        // Keep table lookup parallel but bound pending futures for large unions.
        // Buffered lookups preserve the input order required by UNION.
        use futures::{StreamExt, TryStreamExt, stream};
        let frames: Vec<DataFrame> = stream::iter(names.iter().map(|name| self.table(name)))
            .buffered(16)
            .try_collect()
            .await?;
        let mut frames = frames.into_iter();
        let mut dataframe = frames.next().expect("non-empty table list");
        for frame in frames {
            dataframe = if by_name {
                dataframe.union_by_name(frame)?
            } else {
                dataframe.union(frame)?
            };
        }
        Ok(dataframe)
    }

    /// Union registered tables with matching schemas by column position.
    ///
    /// Duplicate rows are preserved. All sources must have compatible schemas.
    pub async fn union_tables(&self, names: &[&str]) -> Result<DataFrame> {
        self.union_registered_tables(names, false).await
    }

    /// Union registered tables by column position and remove duplicate rows.
    ///
    /// All tables must have compatible schemas.
    pub async fn union_tables_distinct(&self, names: &[&str]) -> Result<DataFrame> {
        self.union_tables(names).await?.distinct()
    }

    /// Union registered tables by column name, filling missing columns with NULL.
    ///
    /// Unlike positional unions, column order may differ and new columns
    /// can appear in later tables. Duplicate rows are preserved.
    pub async fn union_tables_by_name(&self, names: &[&str]) -> Result<DataFrame> {
        self.union_registered_tables(names, true).await
    }

    /// Union registered tables by column name, removing duplicate rows.
    ///
    /// Different column orders and missing columns are supported.
    pub async fn union_tables_distinct_by_name(&self, names: &[&str]) -> Result<DataFrame> {
        self.union_tables_by_name(names).await?.distinct()
    }

    fn validate_dedup_subset(dataframe: &DataFrame, subset: &[&str]) -> Result<()> {
        Self::validate_named_columns(dataframe, subset, "dropDuplicates")
    }

    /// Remove duplicate rows from a registered table, like Spark dropDuplicates.
    ///
    /// An empty subset compares all columns. A non-empty subset groups rows by
    /// the selected columns and keeps an arbitrary row from each group. Use a
    /// stable sort first if a particular representative row is required.
    pub async fn drop_duplicates(&self, table: &str, subset: &[&str]) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr};

        let dataframe = self.table(table).await?;
        if subset.is_empty() {
            return dataframe.distinct();
        }

        Self::validate_dedup_subset(&dataframe, subset)?;

        let keys = subset
            .iter()
            .map(|name| Expr::Column(Column::from_name(*name)))
            .collect();
        let projection = dataframe
            .schema()
            .fields()
            .iter()
            .map(|field| Expr::Column(Column::from_name(field.name())))
            .collect();
        dataframe.distinct_on(keys, projection, None)
    }

    /// Count occurrences of each distinct value in a registered table column.
    ///
    /// Returns a lazy DataFrame with columns named value and frequency. NULL
    /// values form their own group, and the output order is not guaranteed.
    /// The source table is never mutated or collected into memory.
    pub async fn value_counts(&self, table: &str, column: &str) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        let table_name =
            quote_catalog_table(table, "value_counts requires a non-empty table path")?;
        let column_name = quote_join_identifier(column);
        self.sql_read_only(&format!(
            "SELECT {column_name} AS value, COUNT(*) AS frequency FROM {table_name} GROUP BY {column_name}"
        ))
        .await
    }

    /// Count unique whole rows, treating repeated NULL rows as duplicates.
    ///
    /// Unlike row counts, this requires a distinct aggregation over the input.
    pub async fn distinct_row_count(&self, table: &str) -> Result<usize> {
        self.table(table).await?.distinct()?.count().await
    }

    fn sort_dataframe_with_nulls(
        dataframe: DataFrame,
        order: &[(&str, bool, bool)],
    ) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr};
        use std::collections::HashSet;

        let mut seen = HashSet::new();
        let mut expressions = Vec::with_capacity(order.len());
        for &(name, ascending, nulls_first) in order {
            if !seen.insert(name.to_ascii_lowercase()) {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate sort column: {name}"
                )));
            }
            if !dataframe
                .schema()
                .fields()
                .iter()
                .any(|field| field.name().as_str() == name)
            {
                return Err(DataFusionError::Plan(format!(
                    "Unknown sort column: {name}"
                )));
            }
            expressions.push(Expr::Column(Column::from_name(name)).sort(ascending, nulls_first));
        }
        dataframe.sort(expressions)
    }

    fn sort_dataframe(dataframe: DataFrame, order: &[(&str, bool)]) -> Result<DataFrame> {
        let keys = order
            .iter()
            .map(|&(name, ascending)| (name, ascending, ascending))
            .collect::<Vec<_>>();
        Self::sort_dataframe_with_nulls(dataframe, &keys)
    }

    /// Sort rows by multiple named columns using Spark-like NULL ordering.
    ///
    /// A true direction means ascending with NULLs first; false means
    /// descending with NULLs last. Sorts are lazy and do not mutate the table.
    /// Add a unique tie-breaker for deterministic order among equal keys.
    pub async fn sort_rows(&self, table: &str, order: &[(&str, bool)]) -> Result<DataFrame> {
        if order.is_empty() {
            return Err(DataFusionError::Plan(
                "sort_rows requires at least one key".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::sort_dataframe(dataframe, order)
    }

    /// Return the first N rows in the requested sort order.
    ///
    /// Sort and limit are planned lazily. Supply a unique tie-breaker for a
    /// deterministic selection when multiple rows share the same sort keys.
    /// An empty order is rejected; a limit of zero returns no rows.
    pub async fn top_rows(
        &self,
        table: &str,
        order: &[(&str, bool)],
        limit: usize,
    ) -> Result<DataFrame> {
        if order.is_empty() {
            return Err(DataFusionError::Plan(
                "top_rows requires at least one sort key".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::sort_dataframe(dataframe, order)?.limit(0, Some(limit))
    }

    /// Sort registered rows with explicit ascending and NULL placement.
    ///
    /// Each key is (column, ascending, nulls_first). Unlike sort_rows, NULL
    /// placement is independent of sort direction. Rows remain lazy and
    /// equal-key ties have no guaranteed order.
    pub async fn sort_rows_with_nulls(
        &self,
        table: &str,
        order: &[(&str, bool, bool)],
    ) -> Result<DataFrame> {
        if order.is_empty() {
            return Err(DataFusionError::Plan(
                "sort_rows_with_nulls requires at least one key".into(),
            ));
        }
        Self::sort_dataframe_with_nulls(self.table(table).await?, order)
    }

    // Plan independent SQL inputs concurrently, with bounded outstanding
    // work. buffered() returns results in input order, so UNION semantics and
    // the first reported planning error still follow query order.
    async fn plan_union_sql_inputs(&self, queries: &[&str]) -> Result<Vec<DataFrame>> {
        use futures::{StreamExt, TryStreamExt, stream};

        stream::iter(queries.iter().map(|query| self.sql(query)))
            .buffered(16)
            .try_collect()
            .await
    }

    async fn union_sql_internal(&self, queries: &[&str], by_name: bool) -> Result<DataFrame> {
        let operation = if by_name {
            "union_sql_by_name"
        } else {
            "union_sql"
        };
        if queries.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "{operation} requires at least one query"
            )));
        }
        let mut frames = self.plan_union_sql_inputs(queries).await?.into_iter();
        let mut dataframe = frames.next().expect("non-empty query list");
        for frame in frames {
            dataframe = if by_name {
                dataframe.union_by_name(frame)?
            } else {
                dataframe.union(frame)?
            };
        }
        Ok(dataframe)
    }

    /// Union SQL query results by column position.
    ///
    /// Each query must return a compatible schema. Duplicate rows remain.
    pub async fn union_sql(&self, queries: &[&str]) -> Result<DataFrame> {
        self.union_sql_internal(queries, false).await
    }

    /// Union SQL query results by column position and remove duplicates.
    ///
    /// Each query must return compatible columns. NULL rows compare equal
    /// for deduplication. The result is a lazy DataFrame.
    pub async fn union_sql_distinct(&self, queries: &[&str]) -> Result<DataFrame> {
        self.union_sql(queries).await?.distinct()
    }

    /// Union lazy SQL query results by column name.
    ///
    /// Missing fields are filled with NULL and duplicate rows are preserved.
    pub async fn union_sql_by_name(&self, queries: &[&str]) -> Result<DataFrame> {
        self.union_sql_internal(queries, true).await
    }

    /// Union SQL query results by column name and remove duplicate rows.
    ///
    /// Column order can differ, and missing columns are filled with NULL.
    pub async fn union_sql_distinct_by_name(&self, queries: &[&str]) -> Result<DataFrame> {
        self.union_sql_by_name(queries).await?.distinct()
    }

    /// Join registered tables using arbitrary DataFusion expressions.
    ///
    /// Unlike equality-key joins, predicates may be comparisons or multiple
    /// conditions. Every expression is combined with AND. The returned plan is
    /// lazy and supports outer, semi, and anti join types. Use qualified column
    /// references if the two inputs have names in common.
    pub async fn join_tables_on(
        &self,
        left: &str,
        right: &str,
        join_type: datafusion::logical_expr::JoinType,
        on: impl IntoIterator<Item = datafusion::logical_expr::Expr>,
    ) -> Result<DataFrame> {
        let predicates: Vec<_> = on.into_iter().collect();
        if predicates.is_empty() {
            return Err(DataFusionError::Plan(
                "join_tables_on requires at least one predicate; use SQL CROSS JOIN for Cartesian products".into(),
            ));
        }
        // Provider discovery does not depend on the other join input.
        let (left_frame, right_frame) = tokio::try_join!(self.table(left), self.table(right))?;
        left_frame.join_on(right_frame, join_type, predicates)
    }

    /// Join registered tables on equality keys and an extra ON predicate.
    ///
    /// The filter is part of the join condition, not a post-join WHERE clause.
    /// This matters for outer joins because unmatched rows are preserved.
    /// The result is lazy, and semi and anti joins are also supported.
    pub async fn join_tables_filtered(
        &self,
        left: &str,
        right: &str,
        join_type: datafusion::logical_expr::JoinType,
        keys: &[(&str, &str)],
        filter: datafusion::logical_expr::Expr,
    ) -> Result<DataFrame> {
        if keys.is_empty()
            || keys.iter().any(|(left_key, right_key)| {
                left_key.trim().is_empty() || right_key.trim().is_empty()
            })
        {
            return Err(DataFusionError::Plan(
                "join_tables_filtered requires one or more non-empty key pairs".into(),
            ));
        }
        let left_keys: Vec<_> = keys.iter().map(|(key, _)| *key).collect();
        let right_keys: Vec<_> = keys.iter().map(|(_, key)| *key).collect();
        let (left_frame, right_frame) = tokio::try_join!(self.table(left), self.table(right))?;
        left_frame.join(
            right_frame,
            join_type,
            &left_keys,
            &right_keys,
            Some(filter),
        )
    }

    /// Cross join two registered tables, like Spark DataFrame.crossJoin.
    ///
    /// The result has left_rows * right_rows entries. This operation can
    /// produce very large results and should be used deliberately.
    pub async fn cross_join_tables(&self, left: &str, right: &str) -> Result<DataFrame> {
        let query = format!(
            "SELECT * FROM {} AS \"__rustspark_left\" CROSS JOIN {} AS \"__rustspark_right\"",
            quote_catalog_table(
                left,
                "Cross join table name must be a non-empty catalog path",
            )?,
            quote_catalog_table(
                right,
                "Cross join table name must be a non-empty catalog path",
            )?
        );
        self.sql(&query).await
    }

    /// Join values to intervals containing them, with configurable boundaries.
    ///
    /// Values are taken from the left table; interval start/end columns come
    /// from the right table. NULL endpoints do not match. The result is lazy.
    /// INNER and LEFT joins are supported. Non-equality joins can be expensive
    /// on large inputs without another selective predicate.
    pub async fn join_tables_on_range(
        &self,
        values_table: &str,
        intervals_table: &str,
        join_type: datafusion::logical_expr::JoinType,
        value_column: &str,
        interval_columns: (&str, &str),
        bounds: RangeJoinBounds,
    ) -> Result<DataFrame> {
        use datafusion::logical_expr::JoinType;

        if value_column.trim().is_empty()
            || interval_columns.0.trim().is_empty()
            || interval_columns.1.trim().is_empty()
        {
            return Err(DataFusionError::Plan(
                "join_tables_on_range requires non-empty column names".into(),
            ));
        }
        let operator = match join_type {
            JoinType::Inner => "INNER",
            JoinType::Left => "LEFT",
            _ => {
                return Err(DataFusionError::Plan(
                    "join_tables_on_range supports inner and left joins only".into(),
                ));
            }
        };
        let (lower_cmp, upper_cmp) = match bounds {
            RangeJoinBounds::Open => (">", "<"),
            RangeJoinBounds::Closed => (">=", "<="),
            RangeJoinBounds::LeftClosed => (">=", "<"),
            RangeJoinBounds::RightClosed => (">", "<="),
        };
        let value = quote_join_identifier(value_column);
        let lower = quote_join_identifier(interval_columns.0);
        let upper = quote_join_identifier(interval_columns.1);
        let query = format!(
            "SELECT * FROM {} AS \"__rustspark_left\" {operator} JOIN {} AS \"__rustspark_right\" ON \"__rustspark_left\".{value} {lower_cmp} \"__rustspark_right\".{lower} AND \"__rustspark_left\".{value} {upper_cmp} \"__rustspark_right\".{upper}",
            quote_join_table(values_table)?,
            quote_join_table(intervals_table)?,
        );
        self.sql(&query).await
    }

    pub async fn sql(&self, query: &str) -> Result<DataFrame> {
        self.context.sql(query).await
    }

    async fn has_rows(dataframe: DataFrame) -> Result<bool> {
        use futures::StreamExt;

        let mut batches = dataframe.limit(0, Some(1))?.execute_stream().await?;
        while let Some(batch) = batches.next().await {
            if batch?.num_rows() > 0 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Return whether a registered table has at least one row.
    ///
    /// The underlying query is limited to one row, so it does not count or
    /// collect the entire table.
    pub async fn table_has_rows(&self, table: &str) -> Result<bool> {
        Self::has_rows(self.table(table).await?).await
    }

    /// Return whether a read-only query produces at least one row.
    ///
    /// Limits the plan to one row to avoid collecting the full result.
    pub async fn sql_exists(&self, query: &str) -> Result<bool> {
        Self::has_rows(self.sql_read_only(query).await?).await
    }

    /// Check whether parameterized, read-only SQL produces any rows.
    ///
    /// Values are bound without string interpolation and only one row is read.
    pub async fn sql_exists_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<bool> {
        Self::has_rows(self.sql_read_only_with_params(query, params).await?).await
    }

    /// Count all rows produced by read-only SQL without collecting results.
    ///
    /// DDL, DML and session statements are rejected.
    pub async fn sql_read_only_row_count(&self, query: &str) -> Result<usize> {
        self.sql_read_only(query).await?.count().await
    }

    /// Count rows from parameterized read-only SQL with typed bindings.
    pub async fn sql_read_only_row_count_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<usize> {
        self.sql_read_only_with_params(query, params)
            .await?
            .count()
            .await
    }

    async fn extract_scalar(dataframe: DataFrame) -> Result<datafusion::common::ScalarValue> {
        use datafusion::common::ScalarValue;

        if dataframe.schema().fields().len() != 1 {
            return Err(DataFusionError::Plan(
                "Scalar query must return exactly one column".into(),
            ));
        }
        use futures::StreamExt;

        let mut batches = dataframe.limit(0, Some(2))?.execute_stream().await?;
        let mut result = None;
        while let Some(batch) = batches.next().await {
            let batch = batch?;
            if batch.num_rows() > 1 || (batch.num_rows() > 0 && result.is_some()) {
                return Err(DataFusionError::Plan(
                    "Scalar query must return exactly one row".into(),
                ));
            }
            if batch.num_rows() == 1 {
                result = Some(ScalarValue::try_from_array(batch.column(0).as_ref(), 0)?);
            }
        }
        result.ok_or_else(|| DataFusionError::Plan("Scalar query returned no rows".into()))
    }

    /// Execute SQL that must produce exactly one column and one row.
    ///
    /// Reads at most two rows so invalid multi-row queries remain bounded.
    pub async fn sql_scalar(&self, query: &str) -> Result<datafusion::common::ScalarValue> {
        Self::extract_scalar(self.sql(query).await?).await
    }

    /// Execute read-only SQL that must return one row and one column.
    ///
    /// Rejects DDL and DML before execution and reads at most two rows.
    pub async fn sql_read_only_scalar(
        &self,
        query: &str,
    ) -> Result<datafusion::common::ScalarValue> {
        Self::extract_scalar(self.sql_read_only(query).await?).await
    }

    /// Read a single typed SQL value with bound parameters.
    ///
    /// The query must produce exactly one column and one row.
    pub async fn sql_scalar_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<datafusion::common::ScalarValue> {
        Self::extract_scalar(self.sql_with_params(query, params).await?).await
    }

    /// Read one typed scalar from parameterized, read-only SQL.
    ///
    /// The query must return exactly one column and one row. The result is
    /// bounded to two rows and DDL or DML is rejected during planning.
    pub async fn sql_read_only_scalar_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<datafusion::common::ScalarValue> {
        Self::extract_scalar(self.sql_read_only_with_params(query, params).await?).await
    }

    fn validate_page_window(offset: usize, limit: usize) -> Result<()> {
        if limit == 0 {
            return Err(DataFusionError::Plan(
                "SQL page limit must be positive".into(),
            ));
        }
        if offset.checked_add(limit).is_none() {
            return Err(DataFusionError::Plan(
                "SQL page offset plus limit overflows".into(),
            ));
        }
        Ok(())
    }

    async fn paginate<F>(frame: F, offset: usize, limit: usize) -> Result<DataFrame>
    where
        F: std::future::Future<Output = Result<DataFrame>>,
    {
        Self::validate_page_window(offset, limit)?;
        frame.await?.limit(offset, Some(limit))
    }

    /// Plan a paginated query without collecting all its rows.
    ///
    /// Use ORDER BY in the SQL query for deterministic pages.
    pub async fn sql_page(&self, query: &str, offset: usize, limit: usize) -> Result<DataFrame> {
        Self::paginate(self.sql(query), offset, limit).await
    }

    /// Plan a page of read-only SQL without loading the full result.
    ///
    /// Add ORDER BY to the query for deterministic pages.
    pub async fn sql_read_only_page(
        &self,
        query: &str,
        offset: usize,
        limit: usize,
    ) -> Result<DataFrame> {
        Self::paginate(self.sql_read_only(query), offset, limit).await
    }

    /// Plan a parameterized, read-only SQL page without collecting all rows.
    ///
    /// Use ORDER BY in the SQL query for deterministic pages.
    /// Writes and DDL are rejected before binding the parameters.
    pub async fn sql_read_only_page_with_params(
        &self,
        query: &str,
        offset: usize,
        limit: usize,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<DataFrame> {
        Self::paginate(self.sql_read_only_with_params(query, params), offset, limit).await
    }

    /// Plan a parameterized SQL page with bounded row count.
    ///
    /// Use ORDER BY in SQL for stable page boundaries.
    pub async fn sql_page_with_params(
        &self,
        query: &str,
        offset: usize,
        limit: usize,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<DataFrame> {
        Self::paginate(self.sql_with_params(query, params), offset, limit).await
    }

    async fn extract_first_row(
        dataframe: DataFrame,
    ) -> Result<Option<Vec<datafusion::common::ScalarValue>>> {
        use datafusion::common::ScalarValue;

        use futures::StreamExt;

        let mut batches = dataframe.limit(0, Some(1))?.execute_stream().await?;
        while let Some(batch) = batches.next().await {
            let batch = batch?;
            if batch.num_rows() > 0 {
                let values = (0..batch.num_columns())
                    .map(|index| ScalarValue::try_from_array(batch.column(index).as_ref(), 0))
                    .collect::<Result<Vec<_>>>()?;
                return Ok(Some(values));
            }
        }
        Ok(None)
    }

    /// Fetch the first SQL result row as typed Arrow scalar values.
    ///
    /// Returns None for an empty result without collecting the full query.
    pub async fn sql_first_row(
        &self,
        query: &str,
    ) -> Result<Option<Vec<datafusion::common::ScalarValue>>> {
        Self::extract_first_row(self.sql(query).await?).await
    }

    /// Fetch the first row of a read-only SQL query as typed scalar values.
    ///
    /// Returns None for no rows and rejects mutations before execution.
    pub async fn sql_read_only_first_row(
        &self,
        query: &str,
    ) -> Result<Option<Vec<datafusion::common::ScalarValue>>> {
        Self::extract_first_row(self.sql_read_only(query).await?).await
    }

    /// Fetch the first result row from a parameterized SQL query.
    ///
    /// Returns None for an empty result and preserves typed NULL values.
    pub async fn sql_first_row_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<Option<Vec<datafusion::common::ScalarValue>>> {
        Self::extract_first_row(self.sql_with_params(query, params).await?).await
    }

    /// Fetch the first row from parameterized, read-only SQL.
    ///
    /// Returns None for an empty result, preserves typed NULLs, and rejects
    /// DDL or DML without executing the query.
    pub async fn sql_read_only_first_row_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<Option<Vec<datafusion::common::ScalarValue>>> {
        Self::extract_first_row(self.sql_read_only_with_params(query, params).await?).await
    }

    /// Plan SQL with typed positional or named parameters.
    ///
    /// Values are bound by DataFusion rather than interpolated into SQL text.
    /// The returned DataFrame remains lazy until executed.
    pub async fn sql_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<DataFrame> {
        self.sql(query).await?.with_param_values(params)
    }

    /// Plan read-only SQL with typed positional or named parameters.
    ///
    /// Disallowed statement kinds are rejected before binding values.
    pub async fn sql_read_only_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<DataFrame> {
        self.sql_read_only(query).await?.with_param_values(params)
    }

    /// Stream read-only SQL results without collecting all rows.
    ///
    /// DDL, DML and session statements are rejected during planning.
    pub async fn sql_read_only_stream(
        &self,
        query: &str,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        self.sql_read_only(query).await?.execute_stream().await
    }

    /// Stream safely bound SQL results while rejecting writes and DDL.
    ///
    /// The query is validated as read-only before DataFusion binds parameters.
    /// Results are streamed as Arrow batches without collecting every row.
    pub async fn sql_read_only_stream_with_params(
        &self,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        self.sql_read_only_with_params(query, params)
            .await?
            .execute_stream()
            .await
    }

    /// Plan only read-only SQL, rejecting DDL, DML and session statements.
    ///
    /// This is not a replacement for table-level authorization or process isolation.
    pub async fn sql_read_only(&self, query: &str) -> Result<DataFrame> {
        use datafusion::execution::context::SQLOptions;
        let options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        self.context.sql_with_options(query, options).await
    }
}

impl Default for RustSparkSession {
    fn default() -> Self {
        Self::new()
    }
}

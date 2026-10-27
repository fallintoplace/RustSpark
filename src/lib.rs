use std::sync::Arc;

use datafusion::{
    arrow::record_batch::RecordBatch,
    error::{DataFusionError, Result},
    execution::runtime_env::RuntimeEnv,
    prelude::{DataFrame, SessionConfig, SessionContext},
};

mod export;
#[path = "features/group_row_numbers.rs"]
mod feature_group_row_numbers;
mod joins;
#[path = "features/regex_filter.rs"]
mod feature_regex_filter;
mod set_operations;
#[path = "features/null_safe_equality.rs"]
mod feature_null_safe_equality;
mod session_filters;
mod sources;
#[path = "features/top_rows_per_group.rs"]
mod feature_top_rows_per_group;
#[path = "sql_compat/bin.rs"]
mod sql_compat_bin;
#[path = "features/unpivot_columns.rs"]
mod feature_unpivot_columns;
#[path = "sql_compat/endswith.rs"]
mod sql_compat_endswith;
#[path = "features/numeric_column_summary.rs"]
mod feature_numeric_column_summary;
#[path = "sql_compat/nullifzero.rs"]
mod sql_compat_nullifzero;
#[path = "features/approx_distinct_count.rs"]
mod feature_approx_distinct_count;
#[path = "sql_compat/oct.rs"]
mod sql_compat_oct;
#[path = "features/approx_percentile.rs"]
mod feature_approx_percentile;
#[path = "sql_compat/pmod.rs"]
mod sql_compat_pmod;
#[path = "features/substring_filter.rs"]
mod feature_substring_filter;
#[path = "sql_compat/space.rs"]
mod sql_compat_space;
#[path = "features/group_ranks.rs"]
mod feature_group_ranks;
#[path = "sql_compat/startswith.rs"]
mod sql_compat_startswith;
#[path = "sql_compat/try_divide.rs"]
mod sql_compat_try_divide;
#[path = "sql_compat/try_mod.rs"]
mod sql_compat_try_mod;
#[path = "sql_compat/zeroifnull.rs"]
mod sql_compat_zeroifnull;
#[path = "sql_compat/try_add.rs"]
mod sql_compat_try_add;
#[path = "sql_compat/try_subtract.rs"]
mod sql_compat_try_subtract;
#[path = "sql_compat/try_multiply.rs"]
mod sql_compat_try_multiply;
#[path = "sql_compat/try_negate.rs"]
mod sql_compat_try_negate;
#[path = "sql_compat/try_abs.rs"]
mod sql_compat_try_abs;
#[path = "sql_compat/bit_count.rs"]
mod sql_compat_bit_count;
#[path = "sql_compat/shiftleft.rs"]
mod sql_compat_shiftleft;
#[path = "sql_compat/shiftright.rs"]
mod sql_compat_shiftright;
#[path = "sql_compat/bit_get.rs"]
mod sql_compat_bit_get;
#[path = "sql_compat/shiftrightunsigned.rs"]
mod sql_compat_shiftrightunsigned;
#[path = "sql_compat/equal_null.rs"]
mod sql_compat_equal_null;
#[path = "sql_compat/sec.rs"]
mod sql_compat_sec;
mod sql_results;

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
        Self::from_context(SessionContext::new_with_config(config))
    }

    /// Wrap a preconfigured DataFusion context, including an externally
    /// provided execution backend such as Apache DataFusion Ballista.
    ///
    /// RustSpark does not take ownership of any scheduler or executor process.
    pub fn from_context(context: SessionContext) -> Self {
        context.register_udf(sql_compat_endswith::udf());
        context.register_udf(sql_compat_startswith::udf());
        context.register_udf(sql_compat_oct::udf());
        context.register_udf(sql_compat_space::udf());
        context.register_udf(sql_compat_zeroifnull::udf());
        context.register_udf(sql_compat_nullifzero::udf());
        context.register_udf(sql_compat_pmod::udf());
        context.register_udf(sql_compat_try_mod::udf());
        context.register_udf(sql_compat_bin::udf());
        context.register_udf(sql_compat_try_divide::udf());
        context.register_udf(sql_compat_try_add::udf());
        context.register_udf(sql_compat_try_subtract::udf());
        context.register_udf(sql_compat_try_multiply::udf());
        context.register_udf(sql_compat_try_negate::udf());
        context.register_udf(sql_compat_try_abs::udf());
        context.register_udf(sql_compat_bit_count::udf());
        context.register_udf(sql_compat_shiftleft::udf());
        context.register_udf(sql_compat_shiftright::udf());
        context.register_udf(sql_compat_bit_get::udf());
        context.register_udf(sql_compat_shiftrightunsigned::udf());
        context.register_udf(sql_compat_equal_null::udf());
        context.register_udf(sql_compat_sec::udf());
        Self { context }
    }

    pub fn with_config_and_runtime(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        Self::from_context(SessionContext::new_with_config_rt(config, runtime))
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

    /// Plan MIN and MAX for one registered column without collecting rows.
    ///
    /// The result is a lazy single-row DataFrame with min_value and max_value.
    /// NULL inputs are ignored; an empty or all-NULL column produces NULLs.
    pub async fn column_extrema(&self, table: &str, column: &str) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        let source = quote_catalog_table(table, "column_extrema requires a table path")?;
        let key = quote_join_identifier(column);
        self.sql_read_only(&format!(
            "SELECT MIN({key}) AS min_value, MAX({key}) AS max_value FROM {source}"
        ))
        .await
    }

    /// Count the fields of a registered table from its schema only.
    ///
    /// This never executes table data, even for lazy views.
    pub async fn table_column_count(&self, table: &str) -> Result<usize> {
        Ok(self.table(table).await?.schema().fields().len())
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

        use std::fmt::Write as _;
        let mut expressions = String::with_capacity(selected.len() * 48);
        for name in &selected {
            if !expressions.is_empty() {
                expressions.push_str(", ");
            }
            let quoted = quote_join_identifier(name);
            write!(&mut expressions, "COUNT(*) - COUNT({quoted}) AS {quoted}")
                .expect("writing to a String cannot fail");
        }
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

    /// Select all table columns except the named exclusions in source order.
    ///
    /// Unknown, repeated, or all-column exclusions are rejected. A zero-length
    /// exclusion list returns the original lazy table.
    pub async fn select_columns_except(&self, table: &str, excluded: &[&str]) -> Result<DataFrame> {
        let dataframe = self.table(table).await?;
        if excluded.is_empty() {
            return Ok(dataframe);
        }
        Self::validate_selected_columns(&dataframe, excluded)?;
        // Avoid rescanning every exclusion for every field on wide tables.
        let excluded: std::collections::HashSet<&str> = excluded.iter().copied().collect();
        let fields = dataframe.schema().fields().iter().cloned().collect::<Vec<_>>();
        let remaining: Vec<&str> = fields
            .iter()
            .map(|field| field.name().as_str())
            .filter(|name| !excluded.contains(*name))
            .collect();
        if remaining.is_empty() {
            return Err(DataFusionError::Plan(
                "select_columns_except would remove every column".into(),
            ));
        }
        // Copy only shared field references, not the DataFrame's planner state
        // or a String for every surviving column.
        dataframe.select_columns(&remaining)
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

    /// Register a scalar user-defined function for SQL queries in this session.
    pub fn register_udf(&self, udf: datafusion::logical_expr::ScalarUDF) {
        self.context.register_udf(udf);
    }

    async fn filter_rows_pattern(
        &self,
        table: &str,
        column: &str,
        pattern: &str,
        case_insensitive: bool,
    ) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr, prelude::lit};

        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, &[column])?;
        let column = Expr::Column(Column::from_name(column));
        let predicate = if case_insensitive {
            column.ilike(lit(pattern))
        } else {
            column.like(lit(pattern))
        };
        dataframe.filter(predicate)
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

    /// Count rows satisfying a typed filter without collecting result rows.
    ///
    /// SQL three-valued logic applies to the predicate. The table is not
    /// changed and DataFusion plans a COUNT aggregate over the filter.
    pub async fn count_filtered_rows(
        &self,
        table: &str,
        predicate: datafusion::logical_expr::Expr,
    ) -> Result<usize> {
        self.filter_rows(table, predicate).await?.count().await
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
        let fields = dataframe.schema().fields();
        let indexed = (renames.len() > 8).then(|| {
            fields
                .iter()
                .map(|field| field.name().as_str())
                .collect::<HashSet<_>>()
        });
        for source in mappings.keys() {
            let present = match &indexed {
                Some(indexed) => indexed.contains(*source),
                None => fields.iter().any(|field| field.name().as_str() == *source),
            };
            if !present {
                return Err(DataFusionError::Plan(format!(
                    "Unknown column rename source: {source}"
                )));
            }
        }

        let mut names = HashSet::new();
        let mut projection = Vec::with_capacity(fields.len());
        for field in fields {
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
        let fields = dataframe.schema().fields();
        // Sparse replacements should not allocate a full schema index.
        // Large replacement sets still get constant-time membership checks.
        let indexed = (replacements.len() > 8).then(|| {
            fields
                .iter()
                .map(|field| field.name().as_str())
                .collect::<HashSet<_>>()
        });
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
            let present = match &indexed {
                Some(indexed) => indexed.contains(*name),
                None => fields.iter().any(|field| field.name().as_str() == *name),
            };
            if !present {
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

        let fields = dataframe.schema().fields();
        let indexed = (columns.len() > 8).then(|| {
            fields
                .iter()
                .map(|field| (field.name().to_ascii_lowercase(), field.name().as_str()))
                .collect::<HashMap<_, _>>()
        });
        let mut updates = HashMap::new();
        let mut seen = HashSet::new();
        for (name, expr) in columns {
            if name.trim().is_empty() || name.trim() != *name {
                return Err(DataFusionError::Plan(
                    "with_columns requires non-empty, unpadded names".into(),
                ));
            }
            let normalized = name.to_ascii_lowercase();
            if !seen.insert(normalized.clone()) {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate withColumns name: {name}"
                )));
            }
            let conflicting = match &indexed {
                Some(indexed) => indexed
                    .get(&normalized)
                    .is_some_and(|original| original != name),
                None => fields.iter().any(|field| {
                    field.name().as_str() != *name && field.name().eq_ignore_ascii_case(name)
                }),
            };
            if conflicting {
                return Err(DataFusionError::Plan(format!(
                    "with_columns name conflicts with an existing column: {name}"
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

    /// Bernoulli-sample rows with an independent probability per source row.
    ///
    /// The fraction must be finite and in 0..=1. Zero and one avoid random
    /// evaluation. Interior fractions use DataFusion's random() per row.
    /// Samples are lazy, unseeded, and may contain any number of rows.
    pub async fn sample_rows(&self, table: &str, fraction: f64) -> Result<DataFrame> {
        if !fraction.is_finite() || !(0.0..=1.0).contains(&fraction) {
            return Err(DataFusionError::Plan(
                "sample_rows fraction must be finite and between zero and one".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        if fraction == 1.0 {
            return Ok(dataframe);
        }
        if fraction == 0.0 {
            return dataframe.limit(0, Some(0));
        }
        let source = quote_catalog_table(table, "sample_rows requires a table path")?;
        self.sql_read_only(&format!(
            "SELECT * FROM {source} WHERE random() < {fraction}"
        ))
        .await
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

    /// Count occurrences of distinct combinations of several columns.
    ///
    /// Results retain the original grouping column names, plus frequency.
    /// NULLs form groups, and row order is unspecified. The plan is lazy.
    pub async fn group_value_counts(&self, table: &str, columns: &[&str]) -> Result<DataFrame> {
        if columns.is_empty() {
            return Err(DataFusionError::Plan(
                "group_value_counts requires at least one column".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::validate_selected_columns(&dataframe, columns)?;
        if columns
            .iter()
            .any(|name| name.eq_ignore_ascii_case("frequency"))
        {
            return Err(DataFusionError::Plan(
                "group_value_counts reserves the frequency column name".into(),
            ));
        }
        let groups = columns
            .iter()
            .map(|name| quote_join_identifier(name))
            .collect::<Vec<_>>()
            .join(", ");
        let source = quote_catalog_table(table, "group_value_counts requires a table path")?;
        self.sql_read_only(&format!(
            "SELECT {groups}, COUNT(*) AS frequency FROM {source} GROUP BY {groups}"
        ))
        .await
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

    /// Count distinct values in a column with configurable NULL treatment.
    ///
    /// SQL COUNT(DISTINCT) excludes NULL, while Spark-style cardinality may
    /// include one NULL group. Pass include_null to choose the convention.
    /// Rows are aggregated without collecting them in application memory.
    pub async fn column_distinct_count(
        &self,
        table: &str,
        column: &str,
        include_null: bool,
    ) -> Result<usize> {
        let distinct = self.select_columns(table, &[column]).await?.distinct()?;
        if include_null {
            distinct.count().await
        } else {
            use datafusion::{common::Column, logical_expr::Expr};
            distinct
                .filter(Expr::Column(Column::from_name(column)).is_not_null())?
                .count()
                .await
        }
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

        let fields = dataframe.schema().fields();
        // Repeated full-schema searches scale poorly with many sort keys.
        let indexed = (order.len() > 8).then(|| {
            fields
                .iter()
                .map(|field| field.name().as_str())
                .collect::<HashSet<_>>()
        });
        let mut seen = HashSet::new();
        let mut expressions = Vec::with_capacity(order.len());
        for &(name, ascending, nulls_first) in order {
            if !seen.insert(name.to_ascii_lowercase()) {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate sort column: {name}"
                )));
            }
            let present = match &indexed {
                Some(indexed) => indexed.contains(name),
                None => fields.iter().any(|field| field.name().as_str() == name),
            };
            if !present {
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

    /// Select the first N rows under explicit sorting and NULL placement.
    ///
    /// Each order key contains (column, ascending, nulls_first). The sort and
    /// limit remain lazy, and a zero limit returns no rows. Add a unique
    /// tie-breaker when deterministic ordering matters.
    pub async fn top_rows_with_nulls(
        &self,
        table: &str,
        order: &[(&str, bool, bool)],
        limit: usize,
    ) -> Result<DataFrame> {
        if order.is_empty() {
            return Err(DataFusionError::Plan(
                "top_rows_with_nulls requires at least one sort key".into(),
            ));
        }
        let dataframe = self.table(table).await?;
        Self::sort_dataframe_with_nulls(dataframe, order)?.limit(0, Some(limit))
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

    /// Return up to the requested limit of randomly selected rows.
    ///
    /// DataFusion sorts by random() and applies LIMIT. The sample is
    /// unseeded and may be expensive for large tables; use sample_rows
    /// for lower-cost approximate sampling.
    pub async fn take_random_rows(&self, table: &str, limit: usize) -> Result<DataFrame> {
        if limit == 0 {
            return self.table(table).await?.limit(0, Some(0));
        }
        let source = quote_catalog_table(table, "take_random_rows requires a table path")?;
        self.sql_read_only(&format!(
            "SELECT * FROM {source} ORDER BY random() LIMIT {limit}"
        ))
        .await
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

    /// Return the most frequent distinct values from a registered column.
    ///
    /// Computes grouped counts lazily, sorts by descending frequency, and
    /// applies a limit. NULL forms its own group. Ties are unordered.
    /// A limit of zero produces an empty result.
    pub async fn top_frequent_values(
        &self,
        table: &str,
        column: &str,
        limit: usize,
    ) -> Result<DataFrame> {
        use datafusion::{common::Column, logical_expr::Expr};

        let counts = self.value_counts(table, column).await?;
        counts
            .sort(vec![
                Expr::Column(Column::from_name("frequency")).sort(false, false),
            ])?
            .limit(0, Some(limit))
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
}

impl Default for RustSparkSession {
    fn default() -> Self {
        Self::new()
    }
}

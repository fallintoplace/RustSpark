use std::sync::Arc;

use datafusion::{
    arrow::record_batch::RecordBatch,
    error::{DataFusionError, Result},
    execution::runtime_env::RuntimeEnv,
    prelude::{DataFrame, SessionConfig, SessionContext},
};

mod sources;

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

    pub fn with_config_and_runtime(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        Self {
            context: SessionContext::new_with_config_rt(config, runtime),
        }
    }

    /// List registered tables with fully qualified catalog.schema.table names.
    ///
    /// Results are sorted for stable inspection and CLI integration.
    pub fn list_tables(&self) -> Vec<String> {
        let mut tables = Vec::new();
        for catalog_name in self.context.catalog_names() {
            if let Some(catalog) = self.context.catalog(&catalog_name) {
                for schema_name in catalog.schema_names() {
                    if let Some(schema) = catalog.schema(&schema_name) {
                        for table_name in schema.table_names() {
                            tables.push(format!("{catalog_name}.{schema_name}.{table_name}"));
                        }
                    }
                }
            }
        }
        tables.sort();
        tables
    }

    /// List fully qualified catalog.schema names in sorted order.
    ///
    /// Empty schemas are included even when they contain no tables.
    pub fn list_schemas(&self) -> Vec<String> {
        let mut schemas = Vec::new();
        for catalog_name in self.context.catalog_names() {
            if let Some(catalog) = self.context.catalog(&catalog_name) {
                for schema_name in catalog.schema_names() {
                    schemas.push(format!("{catalog_name}.{schema_name}"));
                }
            }
        }
        schemas.sort();
        schemas
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

    /// Register in-memory Arrow batches for SQL queries.
    pub fn register_batches(
        &self,
        table: &str,
        batches: impl IntoIterator<Item = RecordBatch>,
    ) -> Result<()> {
        self.register_dataframe(table, self.read_batches(batches)?)
    }

    /// Register a SQL query as a lazy view in this session.
    ///
    /// The query is planned now but its rows are read when the view is queried.
    pub async fn register_sql_view(&self, name: &str, query: &str) -> Result<()> {
        let dataframe = self.sql(query).await?;
        self.register_dataframe(name, dataframe)
    }

    pub async fn register_materialized_sql_view(&self, name: &str, query: &str) -> Result<()> {
        let dataframe = self.sql(query).await?.cache().await?;
        self.register_dataframe(name, dataframe)
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
        let dataframe = self.sql_with_params(query, params).await?;
        self.register_dataframe(name, dataframe)
    }

    /// Materialize a parameterized SQL query once and register its cached result.
    pub async fn register_materialized_sql_view_with_params(
        &self,
        name: &str,
        query: &str,
        params: impl Into<datafusion::common::ParamValues>,
    ) -> Result<()> {
        let dataframe = self.sql_with_params(query, params).await?.cache().await?;
        self.register_dataframe(name, dataframe)
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

    /// Return column names, data types and nullability without scanning rows.
    pub async fn describe_table(
        &self,
        name: &str,
    ) -> Result<Vec<(String, datafusion::arrow::datatypes::DataType, bool)>> {
        let dataframe = self.table(name).await?;
        Ok(dataframe
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
            .collect())
    }

    /// Inspect the names, types and nullability of a read-only SQL query.
    ///
    /// Query results are not executed or collected.
    pub async fn describe_sql(
        &self,
        query: &str,
    ) -> Result<Vec<(String, datafusion::arrow::datatypes::DataType, bool)>> {
        let dataframe = self.sql_read_only(query).await?;
        Ok(dataframe
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
            .collect())
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
        match self.context.deregister_table(source) {
            Ok(Some(_)) => Ok(()),
            Ok(None) => {
                let _ = self.context.deregister_table(destination);
                Err(DataFusionError::Plan(format!(
                    "Source table disappeared while renaming: {source}"
                )))
            }
            Err(error) => {
                let _ = self.context.deregister_table(destination);
                Err(error)
            }
        }
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

    pub async fn register_table_alias(&self, alias: &str, source: &str) -> Result<()> {
        let dataframe = self.table(source).await?;
        self.register_dataframe(alias, dataframe)
    }

    /// Access a registered SQL table as a lazy DataFrame.
    pub async fn table(&self, name: &str) -> Result<DataFrame> {
        self.context.table(name).await
    }

    /// Union registered tables with matching schemas by column position.
    ///
    /// Duplicate rows are preserved. All sources must have compatible schemas.
    pub async fn union_tables(&self, names: &[&str]) -> Result<DataFrame> {
        let (first, rest) = names.split_first().ok_or_else(|| {
            DataFusionError::Plan("union_tables requires at least one table".into())
        })?;
        let mut dataframe = self.table(first).await?;
        for name in rest {
            dataframe = dataframe.union(self.table(name).await?)?;
        }
        Ok(dataframe)
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
        let (first, rest) = names.split_first().ok_or_else(|| {
            DataFusionError::Plan("union_tables_by_name requires at least one table".into())
        })?;
        let mut dataframe = self.table(first).await?;
        for name in rest {
            dataframe = dataframe.union_by_name(self.table(name).await?)?;
        }
        Ok(dataframe)
    }

    /// Union registered tables by column name, removing duplicate rows.
    ///
    /// Different column orders and missing columns are supported.
    pub async fn union_tables_distinct_by_name(&self, names: &[&str]) -> Result<DataFrame> {
        self.union_tables_by_name(names).await?.distinct()
    }

    /// Union SQL query results by column position.
    ///
    /// Each query must return a compatible schema. Duplicate rows remain.
    pub async fn union_sql(&self, queries: &[&str]) -> Result<DataFrame> {
        let (first, rest) = queries
            .split_first()
            .ok_or_else(|| DataFusionError::Plan("union_sql requires at least one query".into()))?;
        let mut dataframe = self.sql(first).await?;
        for query in rest {
            dataframe = dataframe.union(self.sql(query).await?)?;
        }
        Ok(dataframe)
    }

    /// Union lazy SQL query results by column name.
    ///
    /// Missing fields are filled with NULL and duplicate rows are preserved.
    pub async fn union_sql_by_name(&self, queries: &[&str]) -> Result<DataFrame> {
        let (first, rest) = queries.split_first().ok_or_else(|| {
            DataFusionError::Plan("union_sql_by_name requires at least one query".into())
        })?;
        let mut dataframe = self.sql(first).await?;
        for query in rest {
            dataframe = dataframe.union_by_name(self.sql(query).await?)?;
        }
        Ok(dataframe)
    }

    pub async fn sql(&self, query: &str) -> Result<DataFrame> {
        self.context.sql(query).await
    }

    /// Execute SQL that must produce exactly one column and one row.
    ///
    /// Reads at most two rows so invalid multi-row queries remain bounded.
    pub async fn sql_scalar(&self, query: &str) -> Result<datafusion::common::ScalarValue> {
        use datafusion::common::ScalarValue;

        let dataframe = self.sql(query).await?;
        if dataframe.schema().fields().len() != 1 {
            return Err(DataFusionError::Plan(
                "Scalar query must return exactly one column".into(),
            ));
        }
        let batches = dataframe.limit(0, Some(2))?.collect().await?;
        let mut result = None;
        for batch in batches {
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

    /// Plan a paginated query without collecting all its rows.
    ///
    /// Use ORDER BY in the SQL query for deterministic pages.
    pub async fn sql_page(&self, query: &str, offset: usize, limit: usize) -> Result<DataFrame> {
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
        self.sql(query).await?.limit(offset, Some(limit))
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
        self.sql_with_params(query, params)
            .await?
            .limit(offset, Some(limit))
    }

    /// Fetch the first SQL result row as typed Arrow scalar values.
    ///
    /// Returns None for an empty result without collecting the full query.
    pub async fn sql_first_row(
        &self,
        query: &str,
    ) -> Result<Option<Vec<datafusion::common::ScalarValue>>> {
        use datafusion::common::ScalarValue;

        let batches = self.sql(query).await?.limit(0, Some(1))?.collect().await?;
        for batch in batches {
            if batch.num_rows() > 0 {
                let values = (0..batch.num_columns())
                    .map(|index| ScalarValue::try_from_array(batch.column(index).as_ref(), 0))
                    .collect::<Result<Vec<_>>>()?;
                return Ok(Some(values));
            }
        }
        Ok(None)
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

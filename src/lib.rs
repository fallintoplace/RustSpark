use datafusion::{
    arrow::record_batch::RecordBatch,
    error::{DataFusionError, Result},
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

    /// Create a lazy DataFrame from in-memory Arrow record batches.
    pub fn read_batches(
        &self,
        batches: impl IntoIterator<Item = RecordBatch>,
    ) -> Result<DataFrame> {
        self.context.read_batches(batches)
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

    pub fn register_dataframe(&self, table: &str, dataframe: DataFrame) -> Result<()> {
        if dataframe.task_ctx().session_id() != self.context.session_id() {
            return Err(DataFusionError::Plan(
                "DataFrame belongs to a different RustSpark session".into(),
            ));
        }

        self.context.register_table(table, dataframe.into_view())?;
        Ok(())
    }

    /// Unregister a table or view, returning whether it existed.
    pub fn deregister_table(&self, name: &str) -> Result<bool> {
        Ok(self.context.deregister_table(name)?.is_some())
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

    /// Access a registered SQL table as a lazy DataFrame.
    pub async fn table(&self, name: &str) -> Result<DataFrame> {
        self.context.table(name).await
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

    pub async fn sql(&self, query: &str) -> Result<DataFrame> {
        self.context.sql(query).await
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
}

impl Default for RustSparkSession {
    fn default() -> Self {
        Self::new()
    }
}

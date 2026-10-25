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

    /// Access a registered SQL table as a lazy DataFrame.
    pub async fn table(&self, name: &str) -> Result<DataFrame> {
        self.context.table(name).await
    }

    pub async fn sql(&self, query: &str) -> Result<DataFrame> {
        self.context.sql(query).await
    }
}

impl Default for RustSparkSession {
    fn default() -> Self {
        Self::new()
    }
}

use std::path::Path;

use datafusion::{
    arrow::record_batch::RecordBatch,
    error::{DataFusionError, Result},
    prelude::{
        CsvReadOptions, DataFrame, JsonReadOptions, ParquetReadOptions, SessionConfig,
        SessionContext,
    },
};

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

    pub async fn register_csv(&self, table: &str, path: impl AsRef<Path>) -> Result<()> {
        self.register_csv_with_options(table, path, CsvReadOptions::new())
            .await
    }

    pub async fn register_csv_with_options(
        &self,
        table: &str,
        path: impl AsRef<Path>,
        options: CsvReadOptions<'_>,
    ) -> Result<()> {
        self.context
            .register_csv(table, file_path(path.as_ref())?, options)
            .await
    }

    pub async fn read_csv(&self, path: impl AsRef<Path>) -> Result<DataFrame> {
        self.read_csv_with_options(path, CsvReadOptions::new())
            .await
    }

    pub async fn read_csv_with_options(
        &self,
        path: impl AsRef<Path>,
        options: CsvReadOptions<'_>,
    ) -> Result<DataFrame> {
        self.context
            .read_csv(file_path(path.as_ref())?, options)
            .await
    }

    /// Registers newline-delimited JSON as a SQL table.
    pub async fn register_json(&self, table: &str, path: impl AsRef<Path>) -> Result<()> {
        self.register_json_with_options(table, path, JsonReadOptions::default())
            .await
    }

    pub async fn register_json_with_options(
        &self,
        table: &str,
        path: impl AsRef<Path>,
        options: JsonReadOptions<'_>,
    ) -> Result<()> {
        self.context
            .register_json(table, file_path(path.as_ref())?, options)
            .await
    }

    /// Returns a lazy DataFrame over newline-delimited JSON.
    pub async fn read_json(&self, path: impl AsRef<Path>) -> Result<DataFrame> {
        self.read_json_with_options(path, JsonReadOptions::default())
            .await
    }

    pub async fn read_json_with_options(
        &self,
        path: impl AsRef<Path>,
        options: JsonReadOptions<'_>,
    ) -> Result<DataFrame> {
        self.context
            .read_json(file_path(path.as_ref())?, options)
            .await
    }

    pub async fn register_parquet(&self, table: &str, path: impl AsRef<Path>) -> Result<()> {
        self.register_parquet_with_options(table, path, ParquetReadOptions::new())
            .await
    }

    pub async fn register_parquet_with_options(
        &self,
        table: &str,
        path: impl AsRef<Path>,
        options: ParquetReadOptions<'_>,
    ) -> Result<()> {
        self.context
            .register_parquet(table, file_path(path.as_ref())?, options)
            .await
    }

    pub async fn read_parquet(&self, path: impl AsRef<Path>) -> Result<DataFrame> {
        self.read_parquet_with_options(path, ParquetReadOptions::new())
            .await
    }

    pub async fn read_parquet_with_options(
        &self,
        path: impl AsRef<Path>,
        options: ParquetReadOptions<'_>,
    ) -> Result<DataFrame> {
        self.context
            .read_parquet(file_path(path.as_ref())?, options)
            .await
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

fn file_path(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| DataFusionError::Execution("File path must be valid UTF-8".into()))
}

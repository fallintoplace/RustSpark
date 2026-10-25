use std::path::Path;

use datafusion::{
    error::{DataFusionError, Result},
    prelude::{CsvReadOptions, DataFrame, SessionConfig, SessionContext},
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
            .register_csv(table, csv_path(path.as_ref())?, options)
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
            .read_csv(csv_path(path.as_ref())?, options)
            .await
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

fn csv_path(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| DataFusionError::Execution("CSV path must be valid UTF-8".into()))
}

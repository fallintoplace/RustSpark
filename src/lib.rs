use std::path::Path;

use datafusion::{
    error::{DataFusionError, Result},
    prelude::{CsvReadOptions, DataFrame, SessionContext},
};

pub struct RustSparkSession {
    context: SessionContext,
}

impl RustSparkSession {
    pub fn new() -> Self {
        Self {
            context: SessionContext::new(),
        }
    }

    pub async fn register_csv(&self, table: &str, path: impl AsRef<Path>) -> Result<()> {
        let path = path
            .as_ref()
            .to_str()
            .ok_or_else(|| DataFusionError::Execution("CSV path must be valid UTF-8".into()))?;
        self.context
            .register_csv(table, path, CsvReadOptions::new())
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

use datafusion::{
    error::Result,
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

    pub async fn register_csv(&self, table: &str, path: &str) -> Result<()> {
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

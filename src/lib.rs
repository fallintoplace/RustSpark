//! The small, local SQL execution layer for the RustSpark prototype.
//!
//! This API is not currently compatible with Apache Spark or Spark Connect.

use datafusion::{
    error::Result,
    prelude::{CsvReadOptions, DataFrame, SessionContext},
};

/// A single-process SQL session backed by Apache DataFusion.
pub struct RustSparkSession {
    context: SessionContext,
}

impl RustSparkSession {
    /// Create a new local session with an empty catalog.
    pub fn new() -> Self {
        Self {
            context: SessionContext::new(),
        }
    }

    /// Register a CSV file with a header row as a named SQL table.
    pub async fn register_csv(&self, table: &str, path: &str) -> Result<()> {
        self.context
            .register_csv(table, path, CsvReadOptions::new())
            .await
    }

    /// Plan a SQL statement for lazy execution.
    ///
    /// Call `collect` or `show` on the returned DataFrame to execute it.
    pub async fn sql(&self, query: &str) -> Result<DataFrame> {
        self.context.sql(query).await
    }
}

impl Default for RustSparkSession {
    fn default() -> Self {
        Self::new()
    }
}

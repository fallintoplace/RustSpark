use std::path::Path;

use datafusion::{
    dataframe::DataFrameWriteOptions,
    error::{DataFusionError, Result},
};

use crate::RustSparkSession;

/// On-disk format for a partitioned SQL query export.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlExportFormat {
    Csv,
    Json,
    Parquet,
}

impl RustSparkSession {
    /// Write a read-only query to a directory in CSV, JSON, or Parquet format.
    ///
    /// The returned error covers failed planning or writing. Results are
    /// streamed by DataFusion rather than collected in memory. The destination
    /// is a directory containing one or more part files. Publication is not
    /// atomic, so a failed export may leave incomplete files.
    pub async fn export_sql(
        &self,
        query: &str,
        destination: impl AsRef<Path>,
        format: SqlExportFormat,
    ) -> Result<()> {
        let path = destination
            .as_ref()
            .to_str()
            .ok_or_else(|| DataFusionError::Plan("Output path must be valid UTF-8".into()))?;
        let frame = self.sql_read_only(query).await?;
        let options = DataFrameWriteOptions::new();
        match format {
            SqlExportFormat::Csv => {
                frame.write_csv(path, options, None).await?;
            }
            SqlExportFormat::Json => {
                frame.write_json(path, options, None).await?;
            }
            SqlExportFormat::Parquet => {
                frame.write_parquet(path, options, None).await?;
            }
        }
        Ok(())
    }
}

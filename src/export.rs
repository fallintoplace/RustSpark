use std::{fs, io::ErrorKind, path::Path};

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
    /// Export a read-only SQL result into a new CSV, JSON or Parquet directory.
    ///
    /// The result is written to a temporary sibling first. An existing target
    /// (including a symlink) is rejected, and a failed query removes the stage.
    /// A successful directory rename publishes complete part files together.
    ///
    /// This protects against query errors and cancellation, but does not offer
    /// power-loss durability or coordination with writers outside RustSpark.
    pub async fn export_sql(
        &self,
        query: &str,
        destination: impl AsRef<Path>,
        format: SqlExportFormat,
    ) -> Result<()> {
        let destination = destination.as_ref();
        let parent = destination
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        if destination.file_name().is_none() {
            return Err(DataFusionError::Plan(
                "Output path must name a new directory".into(),
            ));
        }
        reject_existing_export(destination)?;

        // Build on the same filesystem so the final directory rename is atomic.
        let temporary = tempfile::Builder::new()
            .prefix(".rustspark-api-export-")
            .tempdir_in(parent)
            .map_err(external_io)?;
        let staged = temporary.path().join("result");
        let staged_path = staged.to_str().ok_or_else(|| {
            DataFusionError::Plan("Output path must be valid UTF-8".into())
        })?;

        let frame = self.sql_read_only(query).await?;
        let options = DataFrameWriteOptions::new();
        match format {
            SqlExportFormat::Csv => frame.write_csv(staged_path, options, None).await?,
            SqlExportFormat::Json => frame.write_json(staged_path, options, None).await?,
            SqlExportFormat::Parquet => {
                frame.write_parquet(staged_path, options, None).await?
            }
        }
        // A concurrent creator should not be overwritten after our query finishes.
        reject_existing_export(destination)?;
        fs::rename(&staged, destination).map_err(external_io)?;
        Ok(())
    }
}

fn external_io(error: std::io::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn reject_existing_export(destination: &Path) -> Result<()> {
    match fs::symlink_metadata(destination) {
        Ok(_) => Err(DataFusionError::Plan(format!(
            "Output path already exists: {}",
            destination.display()
        ))),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(external_io(error)),
    }
}

use std::path::Path;

use datafusion::{
    error::{DataFusionError, Result},
    prelude::{CsvReadOptions, DataFrame, JsonReadOptions, ParquetReadOptions},
};

use crate::RustSparkSession;

impl RustSparkSession {
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

    /// Read several Parquet files as one lazy DataFrame.
    ///
    /// All inputs must have compatible schemas.
    pub async fn read_parquet_files<P: AsRef<Path>>(&self, paths: &[P]) -> Result<DataFrame> {
        if paths.is_empty() {
            return Err(DataFusionError::Plan("Parquet input needs at least one file".into()));
        }
        let paths: Vec<&str> = paths
            .iter()
            .map(|path| file_path(path.as_ref()))
            .collect::<Result<_>>()?;
        self.context.read_parquet(paths, ParquetReadOptions::new()).await
    }

    /// Register several Parquet files as one SQL table.
    pub async fn register_parquet_files<P: AsRef<Path>>(
        &self,
        table: &str,
        paths: &[P],
    ) -> Result<()> {
        let dataframe = self.read_parquet_files(paths).await?;
        self.register_dataframe(table, dataframe)
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
}

fn file_path(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| DataFusionError::Execution("File path must be valid UTF-8".into()))
}

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

    /// Read several CSV files as one lazy DataFrame.
    ///
    /// All inputs must have compatible schemas.
    pub async fn read_csv_files<P: AsRef<Path>>(&self, paths: &[P]) -> Result<DataFrame> {
        self.read_csv_files_with_options(paths, CsvReadOptions::new())
            .await
    }

    /// Read multiple CSV files with shared delimiter, schema and reader options.
    ///
    /// All inputs must have compatible schemas; no rows are collected here.
    pub async fn read_csv_files_with_options<P: AsRef<Path>>(
        &self,
        paths: &[P],
        options: CsvReadOptions<'_>,
    ) -> Result<DataFrame> {
        let paths = file_paths(paths, "CSV")?;
        self.context.read_csv(paths, options).await
    }

    /// Register several CSV files as a single SQL table.
    pub async fn register_csv_files<P: AsRef<Path>>(&self, table: &str, paths: &[P]) -> Result<()> {
        self.register_csv_files_with_options(table, paths, CsvReadOptions::new())
            .await
    }

    /// Register CSV files as one table with a shared explicit reader configuration.
    pub async fn register_csv_files_with_options<P: AsRef<Path>>(
        &self,
        table: &str,
        paths: &[P],
        options: CsvReadOptions<'_>,
    ) -> Result<()> {
        let dataframe = self.read_csv_files_with_options(paths, options).await?;
        self.register_dataframe(table, dataframe)
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

    /// Read several newline-delimited JSON files as one lazy DataFrame.
    ///
    /// All inputs must have compatible schemas.
    pub async fn read_json_files<P: AsRef<Path>>(&self, paths: &[P]) -> Result<DataFrame> {
        let paths = file_paths(paths, "JSON")?;
        self.context
            .read_json(paths, JsonReadOptions::default())
            .await
    }

    /// Register several newline-delimited JSON files as one SQL table.
    pub async fn register_json_files<P: AsRef<Path>>(
        &self,
        table: &str,
        paths: &[P],
    ) -> Result<()> {
        let dataframe = self.read_json_files(paths).await?;
        self.register_dataframe(table, dataframe)
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
        let paths = file_paths(paths, "Parquet")?;
        self.context
            .read_parquet(paths, ParquetReadOptions::new())
            .await
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

fn file_paths<'a, P: AsRef<Path>>(paths: &'a [P], kind: &str) -> Result<Vec<&'a str>> {
    if paths.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "{kind} input needs at least one file"
        )));
    }
    paths.iter().map(|path| file_path(path.as_ref())).collect()
}

fn file_path(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| DataFusionError::Execution("File path must be valid UTF-8".into()))
}

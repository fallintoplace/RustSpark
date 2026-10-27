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

    pub async fn read_csv_files<P: AsRef<Path>>(&self, paths: &[P]) -> Result<DataFrame> {
        self.read_csv_files_with_options(paths, CsvReadOptions::new())
            .await
    }

    pub async fn read_csv_files_with_options<P: AsRef<Path>>(
        &self,
        paths: &[P],
        options: CsvReadOptions<'_>,
    ) -> Result<DataFrame> {
        let paths = file_paths(paths, "CSV")?;
        self.context.read_csv(paths, options).await
    }

    pub async fn register_csv_files<P: AsRef<Path>>(&self, table: &str, paths: &[P]) -> Result<()> {
        self.register_csv_files_with_options(table, paths, CsvReadOptions::new())
            .await
    }

    pub async fn register_csv_files_with_options<P: AsRef<Path>>(
        &self,
        table: &str,
        paths: &[P],
        options: CsvReadOptions<'_>,
    ) -> Result<()> {
        let dataframe = self.read_csv_files_with_options(paths, options).await?;
        self.register_dataframe(table, dataframe)
    }
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

    pub async fn read_json_files<P: AsRef<Path>>(&self, paths: &[P]) -> Result<DataFrame> {
        self.read_json_files_with_options(paths, JsonReadOptions::default())
            .await
    }

    pub async fn read_json_files_with_options<P: AsRef<Path>>(
        &self,
        paths: &[P],
        options: JsonReadOptions<'_>,
    ) -> Result<DataFrame> {
        let paths = file_paths(paths, "JSON")?;
        self.context.read_json(paths, options).await
    }

    pub async fn register_json_files<P: AsRef<Path>>(
        &self,
        table: &str,
        paths: &[P],
    ) -> Result<()> {
        self.register_json_files_with_options(table, paths, JsonReadOptions::default())
            .await
    }

    pub async fn register_json_files_with_options<P: AsRef<Path>>(
        &self,
        table: &str,
        paths: &[P],
        options: JsonReadOptions<'_>,
    ) -> Result<()> {
        let dataframe = self.read_json_files_with_options(paths, options).await?;
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

    pub async fn read_parquet_files<P: AsRef<Path>>(&self, paths: &[P]) -> Result<DataFrame> {
        self.read_parquet_files_with_options(paths, ParquetReadOptions::new())
            .await
    }

    pub async fn read_parquet_files_with_options<P: AsRef<Path>>(
        &self,
        paths: &[P],
        options: ParquetReadOptions<'_>,
    ) -> Result<DataFrame> {
        let paths = file_paths(paths, "Parquet")?;
        self.context.read_parquet(paths, options).await
    }

    pub async fn register_parquet_files<P: AsRef<Path>>(
        &self,
        table: &str,
        paths: &[P],
    ) -> Result<()> {
        self.register_parquet_files_with_options(table, paths, ParquetReadOptions::new())
            .await
    }

    pub async fn register_parquet_files_with_options<P: AsRef<Path>>(
        &self,
        table: &str,
        paths: &[P],
        options: ParquetReadOptions<'_>,
    ) -> Result<()> {
        let dataframe = self.read_parquet_files_with_options(paths, options).await?;
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
    if path.as_os_str().is_empty() {
        return Err(DataFusionError::Plan("File path cannot be empty".into()));
    }
    path.to_str()
        .ok_or_else(|| DataFusionError::Execution("File path must be valid UTF-8".into()))
}

#[cfg(test)]
mod file_path_tests {
    use super::{file_path, file_paths};
    use std::path::Path;

    #[test]
    fn rejects_empty_single_and_multiple_file_paths() {
        assert!(
            file_path(Path::new(""))
                .unwrap_err()
                .to_string()
                .contains("empty")
        );
        assert!(file_paths(&[Path::new("")], "CSV").is_err());
        assert!(file_paths::<&Path>(&[], "Parquet").is_err());
        assert_eq!(file_path(Path::new("data.csv")).unwrap(), "data.csv");
    }
}

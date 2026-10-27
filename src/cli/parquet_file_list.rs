use std::{
    fs,
    path::{Path, PathBuf},
};

use datafusion::error::{DataFusionError, Result};

/// Parse an explicit list of Parquet parts; keep the data itself lazy.
pub(super) fn load_parquet_file_list(manifest: &Path) -> Result<Vec<PathBuf>> {
    let input = fs::read_to_string(manifest)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let directory = manifest.parent().unwrap_or_else(|| Path::new("."));
    let mut paths = Vec::new();

    for (index, line) in input.lines().enumerate() {
        let name = line.trim();
        if name.is_empty() || name.starts_with('#') {
            continue;
        }
        let candidate = Path::new(name);
        let path = if candidate.is_absolute() {
            candidate.to_path_buf()
        } else {
            directory.join(candidate)
        };
        if !path.is_file() {
            return Err(DataFusionError::Plan(format!(
                "Parquet file list '{}' line {} must name an existing file: {}",
                manifest.display(),
                index + 1,
                path.display()
            )));
        }
        paths.push(path);
    }

    if paths.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "Parquet file list '{}' contains no files",
            manifest.display()
        )));
    }
    Ok(paths)
}

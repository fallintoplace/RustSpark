use std::{
    fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

use datafusion::error::{DataFusionError, Result};

fn parse_manifest_reader(
    reader: impl BufRead,
    directory: &Path,
) -> Result<Vec<(usize, PathBuf)>> {
    let mut paths = Vec::new();
    for (index, line) in reader.lines().enumerate() {
        let line = line.map_err(|error| DataFusionError::External(Box::new(error)))?;
        let line = if index == 0 {
            line.strip_prefix('\u{feff}').unwrap_or(&line)
        } else {
            &line
        };
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
        paths.push((index + 1, path));
    }
    Ok(paths)
}

fn first_missing_file_position(paths: &[(usize, PathBuf)]) -> Option<usize> {
    paths.iter().position(|(_, path)| !path.is_file())
}

// Preserve the earliest invalid manifest entry and report unexpected worker failures.
fn earliest_invalid_worker_result<'scope>(
    checks: impl IntoIterator<Item = std::thread::ScopedJoinHandle<'scope, Option<usize>>>,
) -> Result<Option<usize>> {
    let mut first = None;
    for check in checks {
        let invalid = check.join().map_err(|_| {
            DataFusionError::Execution("Parquet manifest validation worker panicked".into())
        })?;
        if let Some(index) = invalid {
            first = Some(first.map_or(index, |current: usize| current.min(index)));
        }
    }
    Ok(first)
}

fn first_invalid_parallel(paths: &[(usize, PathBuf)], workers: usize) -> Result<Option<usize>> {
    let chunk_size = paths.len().div_ceil(workers);
    std::thread::scope(|scope| {
        let checks = paths
            .chunks(chunk_size)
            .enumerate()
            .map(|(chunk_index, chunk)| {
                scope.spawn(move || {
                    first_missing_file_position(chunk)
                        .map(|index| chunk_index * chunk_size + index)
                })
            })
            .collect::<Vec<_>>();
        earliest_invalid_worker_result(checks)
    })
}

fn first_invalid_path(paths: &[(usize, PathBuf)]) -> Result<Option<usize>> {
    // Check large manifests in parallel while preserving the earliest error.
    let workers = std::thread::available_parallelism()
        .map_or(1, |cores| cores.get())
        .min(8);
    if workers > 1 && paths.len() >= 128 {
        first_invalid_parallel(paths, workers)
    } else {
        Ok(first_missing_file_position(paths))
    }
}

/// Parse an explicit list of Parquet parts; keep the data itself lazy.
pub(super) fn load_parquet_file_list(manifest: &Path) -> Result<Vec<PathBuf>> {
    let input =
        fs::File::open(manifest).map_err(|error| DataFusionError::External(Box::new(error)))?;
    let directory = manifest.parent().unwrap_or_else(|| Path::new("."));
    let paths = parse_manifest_reader(BufReader::new(input), directory)?;

    if paths.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "Parquet file list '{}' contains no files",
            manifest.display()
        )));
    }

    if let Some(index) = first_invalid_path(&paths)? {
        let (line, path) = &paths[index];
        return Err(DataFusionError::Plan(format!(
            "Parquet file list '{}' line {} must name an existing file: {}",
            manifest.display(),
            line,
            path.display()
        )));
    }

    Ok(paths.into_iter().map(|(_, path)| path).collect())
}

#[cfg(test)]
mod tests {
    use super::{earliest_invalid_worker_result, load_parquet_file_list, parse_manifest_reader};
    use std::{fs, io::Cursor, path::Path};

    #[test]
    fn parser_keeps_source_lines_and_relative_paths() {
        let paths = parse_manifest_reader(
            Cursor::new("\u{feff}# header\r\n\r\n part-1.parquet \r\n# skip\npart-2.parquet\n"),
            Path::new("data"),
        ).unwrap();
        assert_eq!(
            paths,
            vec![
                (3, Path::new("data").join("part-1.parquet")),
                (5, Path::new("data").join("part-2.parquet")),
            ]
        );
    }

    #[test]
    fn large_manifest_checks_paths_and_preserves_order() {
        let directory = tempfile::tempdir().unwrap();
        let manifest = directory.path().join("parts.txt");
        let mut lines = String::from("# header\n\n");
        for index in 0..130 {
            let name = format!("part-{index}.parquet");
            fs::write(directory.path().join(&name), []).unwrap();
            lines.push_str(&name);
            lines.push('\n');
        }
        fs::write(&manifest, &lines).unwrap();
        let paths = load_parquet_file_list(&manifest).unwrap();
        assert_eq!(paths.len(), 130);
        assert_eq!(paths[0], directory.path().join("part-0.parquet"));
        assert_eq!(paths[129], directory.path().join("part-129.parquet"));

        // Even when workers discover invalid paths out of order, the first
        // invalid manifest line must win.
        fs::remove_file(directory.path().join("part-2.parquet")).unwrap();
        fs::remove_file(directory.path().join("part-100.parquet")).unwrap();
        let error = load_parquet_file_list(&manifest).unwrap_err();
        assert!(error.to_string().contains("line 5"), "{error}");
    }
    #[test]
    fn accepts_utf8_bom_before_first_path_or_comment() {
        let directory = tempfile::tempdir().unwrap();
        let part = directory.path().join("part.parquet");
        fs::write(&part, []).unwrap();
        let manifest = directory.path().join("parts.txt");

        for contents in [
            "\u{feff}part.parquet\n",
            "\u{feff}# generated file list\npart.parquet\n",
        ] {
            fs::write(&manifest, contents).unwrap();
            assert_eq!(
                load_parquet_file_list(&manifest).unwrap(),
                vec![part.clone()]
            );
        }
    }
    #[test]
    fn worker_results_keep_earliest_invalid_path() {
        std::thread::scope(|scope| {
            let checks = [
                scope.spawn(|| Some(42)),
                scope.spawn(|| Some(3)),
                scope.spawn(|| None),
            ];
            assert_eq!(earliest_invalid_worker_result(checks).unwrap(), Some(3));
        });
    }

    #[test]
    fn worker_panics_return_a_manifest_error() {
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| -> Option<usize> {
                panic!("injected manifest worker panic");
            });
            let error = earliest_invalid_worker_result([worker]).unwrap_err();
            assert!(
                error.to_string().contains("Parquet manifest validation worker panicked"),
                "{error}"
            );
        });
    }

}

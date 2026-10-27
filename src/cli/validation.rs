use super::{Args, StdoutFormat, parse_named_table};

use std::path::{Path, PathBuf};

fn canonical_output_path(destination: &Path) -> Option<PathBuf> {
    if let Ok(existing) = destination.canonicalize() {
        return Some(existing);
    }
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    Some(parent.canonicalize().ok()?.join(destination.file_name()?))
}

fn overlaps_input_path(source: &Path, destination: &Path) -> bool {
    source.canonicalize().is_ok_and(|source| {
        destination == source
            || (source.is_dir() && destination.starts_with(&source))
            || (destination.is_dir() && source.starts_with(destination))
    })
}

fn has_csv_input(args: &Args) -> bool {
    args.csv.is_some() || !args.csv_tables.is_empty()
}

fn has_parquet_input(args: &Args) -> bool {
    args.parquet.is_some() || !args.parquet_tables.is_empty()
}

fn has_json_input(args: &Args) -> bool {
    args.json.is_some() || !args.json_tables.is_empty()
}

fn input_paths(args: &Args) -> Result<Vec<&Path>, Box<dyn std::error::Error>> {
    let mut sources: Vec<&std::path::Path> = Vec::new();
    for path in [
        args.csv.as_ref(),
        args.parquet.as_ref(),
        args.json.as_ref(),
        args.sql_file.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        if path.as_os_str() != "-" {
            sources.push(path.as_path());
        }
    }
    for mapping in args
        .csv_tables
        .iter()
        .chain(&args.parquet_tables)
        .chain(&args.json_tables)
    {
        let (_, path) = parse_named_table(mapping)?;
        sources.push(std::path::Path::new(path));
    }
    Ok(sources)
}

fn reject_output_input_collision(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let Some(destination) = [
        args.output_arrow.as_ref(),
        args.output_parquet.as_ref(),
        args.output_csv.as_ref(),
        args.output_json.as_ref(),
    ]
    .into_iter()
    .flatten()
    .next() else {
        return Ok(());
    };
    let Some(destination) = canonical_output_path(destination) else {
        return Ok(());
    };
    let sources = input_paths(args)?;
    if sources
        .iter()
        .any(|source| overlaps_input_path(source, &destination))
    {
        return Err("Output path must not overwrite an input file or SQL script or be inside an input directory".into());
    }
    Ok(())
}

fn reject_missing_format_inputs(
    options: impl IntoIterator<Item = (bool, bool, &'static str)>,
) -> Result<(), Box<dyn std::error::Error>> {
    for (enabled, has_input, message) in options {
        if enabled && !has_input {
            return Err(message.into());
        }
    }
    Ok(())
}

pub(super) fn validate_inputs(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    if args.output_rows_per_file.is_some()
        && args.output_parquet.is_none()
        && args.output_csv.is_none()
        && args.output_json.is_none()
        && args.database_dir.is_none()
    {
        return Err("--output-rows-per-file requires a file export or --database-dir".into());
    }
    let single_inputs = u8::from(args.csv.is_some())
        + u8::from(args.parquet.is_some())
        + u8::from(args.json.is_some());
    let has_named = !args.csv_tables.is_empty()
        || !args.parquet_tables.is_empty()
        || !args.json_tables.is_empty();
    if args.output_single_file
        && args.output_parquet.is_none()
        && args.output_csv.is_none()
        && args.output_json.is_none()
        && args.output_arrow.is_none()
    {
        return Err("--output-single-file requires an output format".into());
    }
    if single_inputs > 1 || (single_inputs > 0 && has_named) {
        return Err("Use one unnamed input or one or more named tables".into());
    }
    reject_output_input_collision(args)?;
    if args.parquet_pushdown_filters && !has_parquet_input(args) {
        return Err("--parquet-pushdown-filters requires a Parquet input".into());
    }
    Ok(())
}

fn validate_csv_character_options(
    args: &Args,
    has_csv: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let delimiter = args.csv_delimiter.unwrap_or(b',');
    let quote = args.csv_quote.unwrap_or(b'"');
    if delimiter == quote {
        return Err("--csv-quote must differ from the CSV delimiter".into());
    }
    if args.csv_escape.is_some() && !has_csv {
        return Err("--csv-escape requires a CSV input".into());
    }
    if args
        .csv_comment
        .is_some_and(|comment| comment == delimiter || comment == quote)
    {
        return Err("--csv-comment must differ from the CSV delimiter and quote".into());
    }
    if args.csv_escape == Some(delimiter) {
        return Err("--csv-escape must differ from the CSV delimiter".into());
    }
    Ok(())
}

pub(super) fn validate_format_options(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    if args.stdout_no_header
        && !matches!(
            args.stdout_format,
            Some(StdoutFormat::Csv | StdoutFormat::Tsv)
        )
    {
        return Err("--stdout-no-header requires CSV or TSV stdout output".into());
    }
    let csv = has_csv_input(args);
    let parquet = has_parquet_input(args);
    let json = has_json_input(args);

    reject_missing_format_inputs([
        (
            args.csv_delimiter.is_some(),
            csv,
            "--csv-delimiter requires --csv",
        ),
        (
            args.csv_extension.is_some(),
            csv,
            "--csv-extension requires a CSV input",
        ),
        (
            args.parquet_extension.is_some(),
            parquet,
            "--parquet-extension requires a Parquet input",
        ),
        (
            args.parquet_partitions.is_some(),
            parquet,
            "--parquet-partitions requires a Parquet input",
        ),
        (
            args.csv_quote.is_some(),
            csv,
            "--csv-quote requires a CSV input",
        ),
    ])?;
    validate_csv_character_options(args, csv)?;
    reject_missing_format_inputs([
        (
            args.csv_multiline,
            csv,
            "--csv-multiline requires a CSV input",
        ),
        (
            args.csv_allow_short_rows,
            csv,
            "--csv-allow-short-rows requires a CSV input",
        ),
        (
            args.csv_no_header,
            csv,
            "--csv-no-header requires a CSV input",
        ),
        (
            args.csv_schema.is_some(),
            csv,
            "--csv-schema requires a CSV input",
        ),
        (
            args.csv_schema_rows.is_some(),
            csv,
            "--csv-schema-rows requires a CSV input",
        ),
        (
            args.csv_comment.is_some(),
            csv,
            "--csv-comment requires a CSV input",
        ),
        (
            args.json_schema.is_some(),
            json,
            "--json-schema requires a JSON input",
        ),
        (
            args.csv_null_value.is_some(),
            csv,
            "--csv-null-value requires a CSV input",
        ),
        (
            args.json_schema_rows.is_some(),
            json,
            "--json-schema-rows requires a JSON input",
        ),
        (
            args.json_extension.is_some(),
            json,
            "--json-extension requires a JSON input",
        ),
        (args.json_array, json, "--json-array requires a JSON input"),
    ])?;
    Ok(())
}

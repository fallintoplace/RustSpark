use super::{Args, StdoutFormat, parse_named_table};

fn has_csv_input(args: &Args) -> bool {
    args.csv.is_some() || !args.csv_tables.is_empty()
}

fn has_parquet_input(args: &Args) -> bool {
    args.parquet.is_some() || !args.parquet_tables.is_empty()
}

fn has_json_input(args: &Args) -> bool {
    args.json.is_some() || !args.json_tables.is_empty()
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
    let destination = if let Ok(existing) = destination.canonicalize() {
        existing
    } else {
        let parent = destination
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new("."));
        let Ok(parent) = parent.canonicalize() else {
            return Ok(());
        };
        let Some(file_name) = destination.file_name() else {
            return Ok(());
        };
        parent.join(file_name)
    };
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
    if sources.iter().any(|source| {
        source.canonicalize().is_ok_and(|source| {
            destination == source
                || (source.is_dir() && destination.starts_with(&source))
                || (destination.is_dir() && source.starts_with(&destination))
        })
    }) {
        return Err("Output path must not overwrite an input file or SQL script or be inside an input directory".into());
    }
    Ok(())
}

pub(super) fn validate_inputs(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
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

pub(super) fn validate_format_options(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    if args.stdout_no_header
        && !matches!(
            args.stdout_format,
            Some(StdoutFormat::Csv | StdoutFormat::Tsv)
        )
    {
        return Err("--stdout-no-header requires CSV or TSV stdout output".into());
    }
    if args.csv_delimiter.is_some() && !has_csv_input(args) {
        return Err("--csv-delimiter requires --csv".into());
    }
    if args.csv_extension.is_some() && !has_csv_input(args) {
        return Err("--csv-extension requires a CSV input".into());
    }
    if args.parquet_extension.is_some() && !has_parquet_input(args) {
        return Err("--parquet-extension requires a Parquet input".into());
    }
    if args.parquet_partitions.is_some() && !has_parquet_input(args) {
        return Err("--parquet-partitions requires a Parquet input".into());
    }
    if args.csv_quote.is_some() && !has_csv_input(args) {
        return Err("--csv-quote requires a CSV input".into());
    }
    if args.csv_quote == Some(args.csv_delimiter.unwrap_or(b',')) {
        return Err("--csv-quote must differ from the CSV delimiter".into());
    }
    if args.csv_escape.is_some() && !has_csv_input(args) {
        return Err("--csv-escape requires a CSV input".into());
    }
    let delimiter = args.csv_delimiter.unwrap_or(b',');
    let quote = args.csv_quote.unwrap_or(b'"');
    if args
        .csv_comment
        .is_some_and(|comment| comment == delimiter || comment == quote)
    {
        return Err("--csv-comment must differ from the CSV delimiter and quote".into());
    }
    if args.csv_escape == Some(delimiter) {
        return Err("--csv-escape must differ from the CSV delimiter".into());
    }
    if args.csv_multiline && !has_csv_input(args) {
        return Err("--csv-multiline requires a CSV input".into());
    }
    if args.csv_allow_short_rows && !has_csv_input(args) {
        return Err("--csv-allow-short-rows requires a CSV input".into());
    }
    if args.csv_no_header && !has_csv_input(args) {
        return Err("--csv-no-header requires a CSV input".into());
    }
    if args.csv_schema.is_some() && !has_csv_input(args) {
        return Err("--csv-schema requires a CSV input".into());
    }
    if args.csv_schema_rows.is_some() && !has_csv_input(args) {
        return Err("--csv-schema-rows requires a CSV input".into());
    }
    if args.csv_comment.is_some() && !has_csv_input(args) {
        return Err("--csv-comment requires a CSV input".into());
    }
    if args.json_schema.is_some() && !has_json_input(args) {
        return Err("--json-schema requires a JSON input".into());
    }
    if args.csv_null_value.is_some() && !has_csv_input(args) {
        return Err("--csv-null-value requires a CSV input".into());
    }
    if args.json_schema_rows.is_some() && !has_json_input(args) {
        return Err("--json-schema-rows requires a JSON input".into());
    }
    if args.json_extension.is_some() && !has_json_input(args) {
        return Err("--json-extension requires a JSON input".into());
    }
    if args.json_array && !has_json_input(args) {
        return Err("--json-array requires a JSON input".into());
    }
    Ok(())
}

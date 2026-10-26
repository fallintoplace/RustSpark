use datafusion::prelude::{CsvReadOptions, JsonReadOptions, ParquetReadOptions};

use super::Args;

fn csv_null_regex(value: &str) -> String {
    let mut pattern = String::from("^");
    for character in value.chars() {
        if r"\.^$|?*+()[]{}".contains(character) {
            pattern.push('\\');
        }
        pattern.push(character);
    }
    pattern.push('$');
    pattern
}

pub(super) fn csv_read_options(args: &Args) -> CsvReadOptions<'_> {
    let options = CsvReadOptions::new()
        .delimiter(args.csv_delimiter.unwrap_or(b','))
        .quote(args.csv_quote.unwrap_or(b'"'))
        .has_header(!args.csv_no_header)
        .newlines_in_values(args.csv_multiline)
        .truncated_rows(args.csv_allow_short_rows)
        .file_extension(args.csv_extension.as_deref().unwrap_or(".csv"));
    let options = if let Some(schema) = args.csv_schema.as_ref() {
        options.schema(schema)
    } else {
        options
    };
    let options = if let Some(escape) = args.csv_escape {
        options.escape(escape)
    } else {
        options
    };
    let options = if let Some(comment) = args.csv_comment {
        options.comment(comment)
    } else {
        options
    };
    let options = if let Some(value) = args.csv_null_value.as_deref() {
        options.null_regex(Some(csv_null_regex(value)))
    } else {
        options
    };
    if let Some(rows) = args.csv_schema_rows {
        options.schema_infer_max_records(rows)
    } else {
        options
    }
}

pub(super) fn parquet_read_options(args: &Args) -> ParquetReadOptions<'_> {
    let partitions = args
        .parquet_partitions
        .as_ref()
        .map(|schema| {
            schema
                .fields()
                .iter()
                .map(|field| (field.name().clone(), field.data_type().clone()))
                .collect()
        })
        .unwrap_or_default();
    ParquetReadOptions::new()
        .file_extension(args.parquet_extension.as_deref().unwrap_or(".parquet"))
        .table_partition_cols(partitions)
}

pub(super) fn json_read_options(args: &Args) -> JsonReadOptions<'_> {
    let options = JsonReadOptions::default().newline_delimited(!args.json_array);
    let options = if let Some(extension) = args.json_extension.as_deref() {
        options.file_extension(extension)
    } else {
        options
    };
    let options = if let Some(schema) = args.json_schema.as_ref() {
        options.schema(schema)
    } else {
        options
    };
    if let Some(rows) = args.json_schema_rows {
        options.schema_infer_max_records(rows)
    } else {
        options
    }
}

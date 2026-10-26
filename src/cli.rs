use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    collections::{HashMap, HashSet},
    io::{Read, Write},
    path::PathBuf,
    sync::Arc,
};

use clap::{ArgGroup, Parser, ValueEnum};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::common::ScalarValue;
use datafusion::config::TableParquetOptions;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{
    CsvReadOptions, JsonReadOptions, ParquetReadOptions, SessionConfig, cast, ident, lit, nullif,
};
use datafusion::sql::parser::DFParser;
use futures::StreamExt;
use rustspark::RustSparkSession;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum StdoutFormat {
    Csv,
    Json,
}

#[derive(Parser)]
#[command(
    version,
    about = "Run local SQL queries over CSV, JSON or Parquet files",
    group(ArgGroup::new("input").required(false).multiple(true).args(["csv", "parquet", "json", "csv_tables", "parquet_tables", "json_tables"])),
    group(ArgGroup::new("query").required(true).args(["sql", "sql_file"]))
)]
struct Args {
    #[arg(long, value_name = "PATH")]
    csv: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    parquet: Option<PathBuf>,

    #[arg(long = "csv-table", value_name = "NAME=PATH")]
    csv_tables: Vec<String>,

    #[arg(long = "parquet-table", value_name = "NAME=PATH")]
    parquet_tables: Vec<String>,

    #[arg(long, value_name = "CHAR", value_parser = parse_delimiter)]
    csv_delimiter: Option<u8>,

    #[arg(long = "csv-extension", value_name = ".EXT", value_parser = parse_file_extension)]
    csv_extension: Option<String>,

    #[arg(long = "parquet-extension", value_name = ".EXT", value_parser = parse_file_extension)]
    parquet_extension: Option<String>,

    #[arg(long = "parquet-partitions", value_name = "NAME:TYPE,...", value_parser = parse_parquet_partitions)]
    parquet_partitions: Option<Schema>,

    #[arg(long = "parquet-pushdown-filters")]
    parquet_pushdown_filters: bool,

    #[arg(
        long = "parquet-reorder-filters",
        requires = "parquet_pushdown_filters"
    )]
    parquet_reorder_filters: bool,

    #[arg(long = "csv-quote", value_name = "CHAR", value_parser = parse_csv_char)]
    csv_quote: Option<u8>,

    #[arg(long = "csv-escape", value_name = "CHAR", value_parser = parse_csv_char)]
    csv_escape: Option<u8>,

    #[arg(long = "csv-multiline")]
    csv_multiline: bool,

    #[arg(long = "csv-allow-short-rows")]
    csv_allow_short_rows: bool,

    #[arg(long = "csv-no-header")]
    csv_no_header: bool,

    #[arg(long = "csv-comment", value_name = "CHAR", value_parser = parse_delimiter)]
    csv_comment: Option<u8>,

    #[arg(long = "csv-null-value", value_name = "TEXT", value_parser = parse_csv_null_value)]
    csv_null_value: Option<String>,

    #[arg(long = "csv-schema", value_name = "NAME:TYPE,...", value_parser = parse_csv_schema)]
    csv_schema: Option<Schema>,

    #[arg(long = "csv-schema-rows", value_name = "ROWS", value_parser = parse_csv_schema_rows)]
    csv_schema_rows: Option<usize>,

    #[arg(long, value_name = "PATH")]
    json: Option<PathBuf>,

    #[arg(long = "json-schema", value_name = "NAME:TYPE,...", value_parser = parse_json_schema)]
    json_schema: Option<Schema>,

    #[arg(long = "json-schema-rows", value_name = "ROWS", value_parser = parse_json_schema_rows)]
    json_schema_rows: Option<usize>,

    #[arg(long = "json-extension", value_name = ".EXT", value_parser = parse_file_extension)]
    json_extension: Option<String>,

    #[arg(long = "json-array")]
    json_array: bool,

    #[arg(long = "json-table", value_name = "NAME=PATH")]
    json_tables: Vec<String>,

    #[arg(long, default_value = "data", value_parser = parse_table_name, conflicts_with_all = ["csv_tables", "parquet_tables", "json_tables"])]
    table: String,

    #[arg(long = "cache-table", value_name = "NAME")]
    cache_tables: Vec<String>,

    #[arg(long = "union-by-name", value_name = "NAME=TABLE1,TABLE2,...")]
    unions_by_name: Vec<String>,

    #[arg(long, value_name = "QUERY")]
    sql: Option<String>,

    #[arg(long, value_name = "PATH")]
    sql_file: Option<PathBuf>,

    #[arg(long = "sql-view", value_name = "NAME=SQL")]
    sql_views: Vec<String>,

    #[arg(long = "materialized-sql-view", value_name = "NAME=SQL")]
    materialized_sql_views: Vec<String>,

    #[arg(long = "param", value_name = "TYPE:VALUE", value_parser = parse_sql_param)]
    params: Vec<ScalarValue>,

    #[arg(
        long = "named-param",
        value_name = "NAME=TYPE:VALUE",
        value_parser = parse_named_param,
        conflicts_with = "params"
    )]
    named_params: Vec<(String, ScalarValue)>,

    #[arg(
        long = "describe",
        conflicts_with_all = ["max_rows", "output_parquet", "output_csv", "output_json", "output_arrow"]
    )]
    describe: bool,

    #[arg(
        long = "explain",
        conflicts_with_all = ["describe", "max_rows", "output_parquet", "output_csv", "output_json", "output_arrow"]
    )]
    explain: bool,

    #[arg(
        long = "explain-analyze",
        conflicts_with_all = ["describe", "explain", "max_rows", "output_parquet", "output_csv", "output_json", "output_arrow"]
    )]
    explain_analyze: bool,

    #[arg(long, value_name = "ROWS", value_parser = parse_positive_rows)]
    max_rows: Option<usize>,

    #[arg(long, value_name = "ROWS", requires = "max_rows")]
    offset: Option<usize>,

    #[arg(
        long = "stdout-format",
        value_enum,
        value_name = "FORMAT",
        conflicts_with_all = ["describe", "explain", "explain_analyze", "count_only", "output_parquet", "output_csv", "output_json", "output_arrow"]
    )]
    stdout_format: Option<StdoutFormat>,

    #[arg(
        long = "count-only",
        conflicts_with_all = ["describe", "explain", "explain_analyze", "max_rows", "output_parquet", "output_csv", "output_json", "output_arrow"]
    )]
    count_only: bool,

    #[arg(long = "query-timeout", value_name = "SECONDS", value_parser = parse_query_timeout)]
    query_timeout: Option<u64>,

    #[arg(long = "memory-limit-mb", value_name = "MIB", value_parser = parse_memory_limit_mib)]
    memory_limit_bytes: Option<usize>,

    #[arg(long = "batch-size", value_name = "ROWS", value_parser = parse_batch_size)]
    batch_size: Option<usize>,

    #[arg(long = "target-partitions", value_name = "PARTITIONS", value_parser = parse_target_partitions)]
    target_partitions: Option<usize>,

    #[arg(
        long = "output-parquet",
        value_name = "DIR",
        conflicts_with = "max_rows"
    )]
    output_parquet: Option<PathBuf>,

    #[arg(
        long = "output-csv",
        value_name = "DIR",
        conflicts_with_all = ["max_rows", "output_parquet"]
    )]
    output_csv: Option<PathBuf>,

    #[arg(
        long = "output-json",
        value_name = "DIR",
        conflicts_with_all = ["max_rows", "output_parquet", "output_csv"]
    )]
    output_json: Option<PathBuf>,

    #[arg(
        long = "output-arrow",
        value_name = "FILE",
        conflicts_with_all = ["max_rows", "output_parquet", "output_csv", "output_json"]
    )]
    output_arrow: Option<PathBuf>,

    #[arg(long = "output-single-file")]
    output_single_file: bool,

    #[arg(
        long = "parquet-compression",
        value_name = "CODEC",
        requires = "output_parquet"
    )]
    parquet_compression: Option<String>,
}

fn parse_delimiter(value: &str) -> Result<u8, String> {
    match value.as_bytes() {
        [byte] if *byte != b'\n' && *byte != b'\r' && *byte != b'"' => Ok(*byte),
        _ => Err("CSV delimiter must be one ASCII character other than a quote or newline".into()),
    }
}

fn parse_csv_char(value: &str) -> Result<u8, String> {
    match value.as_bytes() {
        [byte] if *byte != b'\n' && *byte != b'\r' => Ok(*byte),
        _ => Err("CSV character must be one ASCII character other than newline".into()),
    }
}

fn parse_csv_null_value(value: &str) -> Result<String, String> {
    if value.is_empty() || value.contains('\n') || value.contains('\r') {
        Err("CSV null marker cannot be empty or contain a newline".into())
    } else {
        Ok(value.to_owned())
    }
}

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

fn parse_file_extension(value: &str) -> Result<String, String> {
    if value.len() > 1
        && value.starts_with('.')
        && value[1..].bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        Ok(value.to_owned())
    } else {
        Err("Extension must start with '.' and contain only ASCII letters or digits".into())
    }
}

fn valid_identifier(name: &str) -> bool {
    name.bytes()
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn parse_table_name(value: &str) -> Result<String, String> {
    if valid_identifier(value) {
        Ok(value.to_owned())
    } else {
        Err("--table must be a SQL identifier".into())
    }
}

fn parse_named_table(value: &str) -> Result<(&str, &str), String> {
    let (name, path) = value
        .split_once('=')
        .ok_or_else(|| "Named table must use NAME=PATH".to_owned())?;
    if !valid_identifier(name) || path.is_empty() {
        return Err("Named table needs a SQL identifier and non-empty path".into());
    }
    Ok((name, path))
}

fn parse_union_by_name(value: &str) -> Result<(&str, Vec<&str>), String> {
    let (name, source_names) = parse_named_table(value)
        .map_err(|_| "--union-by-name requires NAME=TABLE1,TABLE2,...".to_owned())?;
    let sources: Vec<_> = source_names.split(',').collect();
    if sources.len() < 2 || sources.iter().any(|source| !valid_identifier(source)) {
        return Err("--union-by-name requires at least two valid table names".into());
    }
    Ok((name, sources))
}

fn parse_named_view(value: &str) -> Result<(&str, &str), String> {
    let (name, sql) = parse_named_table(value)
        .map_err(|_| "--sql-view must use NAME=SQL with a valid SQL identifier".to_owned())?;
    if sql.trim().is_empty() {
        return Err("--sql-view SQL query cannot be empty".into());
    }
    Ok((name, sql))
}

fn parse_column_schema(value: &str, source: &str) -> Result<Schema, String> {
    let mut fields = Vec::new();
    let mut names = HashSet::new();
    for entry in value.split(',') {
        let (name, kind) = entry
            .split_once(':')
            .ok_or_else(|| format!("{source} schema needs NAME:TYPE entries"))?;
        let valid_first = name
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_');
        if !valid_first
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(format!(
                "{source} schema column names must be SQL identifiers"
            ));
        }
        if !names.insert(name.to_ascii_lowercase()) {
            return Err(format!("Duplicate {source} schema column: {name}"));
        }
        let data_type = match kind.to_ascii_lowercase().as_str() {
            "utf8" | "string" => DataType::Utf8,
            "int8" | "tinyint" => DataType::Int8,
            "int16" | "smallint" => DataType::Int16,
            "int32" | "int" | "integer" => DataType::Int32,
            "int64" | "bigint" => DataType::Int64,
            "uint8" => DataType::UInt8,
            "uint16" => DataType::UInt16,
            "uint32" => DataType::UInt32,
            "uint64" => DataType::UInt64,
            "float32" | "float" | "real" => DataType::Float32,
            "float64" | "double" => DataType::Float64,
            "bool" | "boolean" => DataType::Boolean,
            "date32" | "date" => DataType::Date32,
            _ => return Err(format!("Unsupported {source} type: {kind}")),
        };
        fields.push(Field::new(name, data_type, true));
    }
    Ok(Schema::new(fields))
}

fn parse_sql_param(value: &str) -> Result<ScalarValue, String> {
    let (kind, raw) = value
        .split_once(':')
        .ok_or_else(|| "--param must use TYPE:VALUE".to_owned())?;
    match kind.to_ascii_lowercase().as_str() {
        "int8" | "tinyint" => raw
            .parse::<i8>()
            .map(|number| ScalarValue::Int8(Some(number)))
            .map_err(|_| "--param int8 needs a signed 8-bit integer".to_owned()),
        "int16" | "smallint" => raw
            .parse::<i16>()
            .map(|number| ScalarValue::Int16(Some(number)))
            .map_err(|_| "--param int16 needs a signed 16-bit integer".to_owned()),
        "int32" | "int" | "integer" => raw
            .parse::<i32>()
            .map(|number| ScalarValue::Int32(Some(number)))
            .map_err(|_| "--param int32 needs a signed 32-bit integer".to_owned()),
        "int64" | "bigint" => raw
            .parse::<i64>()
            .map(|number| ScalarValue::Int64(Some(number)))
            .map_err(|_| "--param int64 needs a signed 64-bit integer".to_owned()),
        "uint8" => raw
            .parse::<u8>()
            .map(|number| ScalarValue::UInt8(Some(number)))
            .map_err(|_| "--param uint8 needs an unsigned 8-bit integer".to_owned()),
        "uint16" => raw
            .parse::<u16>()
            .map(|number| ScalarValue::UInt16(Some(number)))
            .map_err(|_| "--param uint16 needs an unsigned 16-bit integer".to_owned()),
        "uint32" => raw
            .parse::<u32>()
            .map(|number| ScalarValue::UInt32(Some(number)))
            .map_err(|_| "--param uint32 needs an unsigned 32-bit integer".to_owned()),
        "uint64" => raw
            .parse::<u64>()
            .map(|number| ScalarValue::UInt64(Some(number)))
            .map_err(|_| "--param uint64 needs an unsigned 64-bit integer".to_owned()),
        "float32" | "real" => {
            let number = raw
                .parse::<f32>()
                .map_err(|_| "--param float32 needs a number".to_owned())?;
            if !number.is_finite() {
                return Err("--param float32 must be finite".into());
            }
            Ok(ScalarValue::Float32(Some(number)))
        }
        "float64" | "double" => {
            let number = raw
                .parse::<f64>()
                .map_err(|_| "--param float64 needs a number".to_owned())?;
            if !number.is_finite() {
                return Err("--param float64 must be finite".into());
            }
            Ok(ScalarValue::Float64(Some(number)))
        }
        "bool" | "boolean" => raw
            .parse::<bool>()
            .map(|value| ScalarValue::Boolean(Some(value)))
            .map_err(|_| "--param bool must be true or false".to_owned()),
        "utf8" | "string" => Ok(ScalarValue::Utf8(Some(raw.to_owned()))),
        "null" if raw.is_empty() => Ok(ScalarValue::Null),
        _ => Err(
            "--param supports int8/16/32/64, uint8/16/32/64, float32/64, bool, utf8, or null:"
                .into(),
        ),
    }
}

fn parse_named_param(value: &str) -> Result<(String, ScalarValue), String> {
    let (name, raw) = value
        .split_once('=')
        .filter(|(name, _)| valid_identifier(name))
        .ok_or_else(|| {
            "--named-param requires NAME=TYPE:VALUE and a valid identifier".to_owned()
        })?;
    let value = parse_sql_param(raw).map_err(|error| error.replace("--param", "--named-param"))?;
    Ok((name.to_owned(), value))
}

fn parse_parquet_partitions(value: &str) -> Result<Schema, String> {
    parse_column_schema(value, "Parquet partition")
}

fn parse_csv_schema(value: &str) -> Result<Schema, String> {
    parse_column_schema(value, "CSV")
}

fn parse_json_schema(value: &str) -> Result<Schema, String> {
    parse_column_schema(value, "JSON")
}

fn parse_positive_usize(value: &str, option: &str) -> Result<usize, String> {
    let count = value
        .parse::<usize>()
        .map_err(|_| format!("{option} requires a positive integer"))?;
    if count == 0 {
        return Err(format!("{option} must be greater than zero"));
    }
    Ok(count)
}

fn parse_csv_schema_rows(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--csv-schema-rows")
}

fn parse_json_schema_rows(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--json-schema-rows")
}

fn csv_read_options(args: &Args) -> CsvReadOptions<'_> {
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

fn parquet_read_options(args: &Args) -> ParquetReadOptions<'_> {
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

fn json_read_options(args: &Args) -> JsonReadOptions<'_> {
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

async fn reject_duplicate_csv_headers(
    session: &RustSparkSession,
    name: &str,
) -> datafusion::error::Result<()> {
    let dataframe = session.table(name).await?;
    let mut names = HashSet::new();
    for field in dataframe.schema().fields() {
        if !names.insert(field.name().to_ascii_lowercase()) {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "Duplicate CSV column: {}",
                field.name()
            )));
        }
    }
    Ok(())
}

// DataFusion 55 uses null_regex for CSV inference but not for decoding rows.
// Read as strings and normalize before casting to the inferred input types.
async fn register_csv_with_null_value(
    session: &RustSparkSession,
    name: &str,
    path: impl AsRef<std::path::Path>,
    options: CsvReadOptions<'_>,
    marker: &str,
) -> datafusion::error::Result<()> {
    let inferred = session
        .read_csv_with_options(&path, options.clone())
        .await?;
    let fields = inferred.schema().fields();
    let string_schema = Schema::new(
        fields
            .iter()
            .map(|field| Field::new(field.name(), DataType::Utf8, true))
            .collect::<Vec<_>>(),
    );
    let raw = session
        .read_csv_with_options(path, options.schema(&string_schema).null_regex(None))
        .await?;
    let expressions = fields
        .iter()
        .map(|field| {
            let value = nullif(ident(field.name()), lit(marker));
            let typed = if field.data_type() == &DataType::Utf8 {
                value
            } else {
                cast(value, field.data_type().clone())
            };
            typed.alias(field.name())
        })
        .collect::<Vec<_>>();
    session.register_dataframe(name, raw.select(expressions)?)?;
    Ok(())
}

fn parse_query_timeout(value: &str) -> Result<u64, String> {
    let seconds = value
        .parse::<u64>()
        .map_err(|_| "--query-timeout requires a positive integer in seconds".to_owned())?;
    if seconds == 0 {
        return Err("--query-timeout must be greater than zero".into());
    }
    Ok(seconds)
}

fn parse_memory_limit_mib(value: &str) -> Result<usize, String> {
    let mib = value
        .parse::<usize>()
        .map_err(|_| "--memory-limit-mb requires a positive integer".to_owned())?;
    mib.checked_mul(1024 * 1024)
        .filter(|bytes| *bytes > 0)
        .ok_or_else(|| "--memory-limit-mb must fit in a positive number of bytes".to_owned())
}

fn parse_batch_size(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--batch-size")
}

fn parse_target_partitions(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--target-partitions")
}

fn parse_positive_rows(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--max-rows")
}

fn output_write_options(single_file: bool) -> DataFrameWriteOptions {
    let options = DataFrameWriteOptions::new();
    if single_file {
        options.with_single_file_output(true)
    } else {
        options
    }
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
            destination == source || (source.is_dir() && destination.starts_with(&source))
        })
    }) {
        return Err("Output path must not overwrite an input file or SQL script or be inside an input directory".into());
    }
    Ok(())
}

pub(crate) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = Arc::new(Args::parse());
    let mut parameter_names = HashSet::new();
    for (name, _) in &args.named_params {
        if !parameter_names.insert(name.as_str()) {
            return Err(format!("Duplicate named parameter: {name}").into());
        }
    }
    if let (Some(offset), Some(max_rows)) = (args.offset, args.max_rows) {
        offset
            .checked_add(max_rows)
            .ok_or("--offset plus --max-rows exceeds the supported range")?;
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
    reject_output_input_collision(&args)?;
    if args.parquet_pushdown_filters && args.parquet.is_none() && args.parquet_tables.is_empty() {
        return Err("--parquet-pushdown-filters requires a Parquet input".into());
    }
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = args.parquet_pushdown_filters;
    config.options_mut().execution.parquet.reorder_filters = args.parquet_reorder_filters;
    let config = if let Some(batch_size) = args.batch_size {
        config.with_batch_size(batch_size)
    } else {
        config
    };
    let config = if let Some(partitions) = args.target_partitions {
        config.with_target_partitions(partitions)
    } else {
        config
    };
    let session = if let Some(limit) = args.memory_limit_bytes {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(limit, 1.0)
            .build_arc()?;
        Arc::new(RustSparkSession::with_config_and_runtime(config, runtime))
    } else {
        Arc::new(RustSparkSession::with_config(config))
    };
    if args.csv_delimiter.is_some() && args.csv.is_none() && args.csv_tables.is_empty() {
        return Err("--csv-delimiter requires --csv".into());
    }
    if args.csv_extension.is_some() && args.csv.is_none() && args.csv_tables.is_empty() {
        return Err("--csv-extension requires a CSV input".into());
    }
    if args.parquet_extension.is_some() && args.parquet.is_none() && args.parquet_tables.is_empty()
    {
        return Err("--parquet-extension requires a Parquet input".into());
    }
    if args.parquet_partitions.is_some() && args.parquet.is_none() && args.parquet_tables.is_empty()
    {
        return Err("--parquet-partitions requires a Parquet input".into());
    }
    if args.csv_quote.is_some() && args.csv.is_none() && args.csv_tables.is_empty() {
        return Err("--csv-quote requires a CSV input".into());
    }
    if args.csv_quote == Some(args.csv_delimiter.unwrap_or(b',')) {
        return Err("--csv-quote must differ from the CSV delimiter".into());
    }
    if args.csv_escape.is_some() && args.csv.is_none() && args.csv_tables.is_empty() {
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
    if args.csv_multiline && args.csv.is_none() && args.csv_tables.is_empty() {
        return Err("--csv-multiline requires a CSV input".into());
    }
    if args.csv_allow_short_rows && args.csv.is_none() && args.csv_tables.is_empty() {
        return Err("--csv-allow-short-rows requires a CSV input".into());
    }
    if args.csv_no_header && args.csv.is_none() && args.csv_tables.is_empty() {
        return Err("--csv-no-header requires a CSV input".into());
    }
    if args.csv_schema.is_some() && args.csv.is_none() && args.csv_tables.is_empty() {
        return Err("--csv-schema requires a CSV input".into());
    }
    if args.csv_schema_rows.is_some() && args.csv.is_none() && args.csv_tables.is_empty() {
        return Err("--csv-schema-rows requires a CSV input".into());
    }
    if args.csv_comment.is_some() && args.csv.is_none() && args.csv_tables.is_empty() {
        return Err("--csv-comment requires a CSV input".into());
    }
    if args.json_schema.is_some() && args.json.is_none() && args.json_tables.is_empty() {
        return Err("--json-schema requires a JSON input".into());
    }
    if args.csv_null_value.is_some() && args.csv.is_none() && args.csv_tables.is_empty() {
        return Err("--csv-null-value requires a CSV input".into());
    }
    if args.json_schema_rows.is_some() && args.json.is_none() && args.json_tables.is_empty() {
        return Err("--json-schema-rows requires a JSON input".into());
    }
    if args.json_extension.is_some() && args.json.is_none() && args.json_tables.is_empty() {
        return Err("--json-extension requires a JSON input".into());
    }
    if args.json_array && args.json.is_none() && args.json_tables.is_empty() {
        return Err("--json-array requires a JSON input".into());
    }
    if let Some(csv) = args.csv.as_ref() {
        let options = csv_read_options(&args);
        if let Some(marker) = args.csv_null_value.as_deref() {
            register_csv_with_null_value(&session, &args.table, csv, options, marker).await?;
        } else {
            session
                .register_csv_with_options(&args.table, csv, options)
                .await?;
        }
        reject_duplicate_csv_headers(&session, &args.table).await?;
    } else if let Some(parquet) = args.parquet.as_ref() {
        let options = parquet_read_options(&args);
        session
            .register_parquet_with_options(&args.table, parquet, options)
            .await?;
    } else if let Some(json) = args.json.as_ref() {
        let options = json_read_options(&args);
        session
            .register_json_with_options(&args.table, json, options)
            .await?;
    } else {
        let mut names = HashSet::new();
        let mut registration_jobs = tokio::task::JoinSet::new();
        for mapping in &args.csv_tables {
            let (name, path) = parse_named_table(mapping)?;
            if !names.insert(name.to_ascii_lowercase()) {
                return Err(format!("Duplicate table name: {name}").into());
            }
            let session = Arc::clone(&session);
            let args = Arc::clone(&args);
            let name = name.to_owned();
            let path = path.to_owned();
            registration_jobs.spawn(async move {
                let options = csv_read_options(&args);
                if let Some(marker) = args.csv_null_value.as_deref() {
                    register_csv_with_null_value(&session, &name, &path, options, marker).await?;
                } else {
                    session
                        .register_csv_with_options(&name, &path, options)
                        .await?;
                }
                reject_duplicate_csv_headers(&session, &name).await
            });
            if registration_jobs.len() >= 4 {
                registration_jobs
                    .join_next()
                    .await
                    .expect("registration task exists")??;
            }
        }
        for mapping in &args.parquet_tables {
            let (name, path) = parse_named_table(mapping)?;
            if !names.insert(name.to_ascii_lowercase()) {
                return Err(format!("Duplicate table name: {name}").into());
            }
            let session = Arc::clone(&session);
            let args = Arc::clone(&args);
            let name = name.to_owned();
            let path = path.to_owned();
            registration_jobs.spawn(async move {
                let options = parquet_read_options(&args);
                session
                    .register_parquet_with_options(&name, &path, options)
                    .await
            });
            if registration_jobs.len() >= 4 {
                registration_jobs
                    .join_next()
                    .await
                    .expect("registration task exists")??;
            }
        }
        for mapping in &args.json_tables {
            let (name, path) = parse_named_table(mapping)?;
            if !names.insert(name.to_ascii_lowercase()) {
                return Err(format!("Duplicate table name: {name}").into());
            }
            let session = Arc::clone(&session);
            let args = Arc::clone(&args);
            let name = name.to_owned();
            let path = path.to_owned();
            registration_jobs.spawn(async move {
                let options = json_read_options(&args);
                session
                    .register_json_with_options(&name, &path, options)
                    .await
            });
            if registration_jobs.len() >= 4 {
                registration_jobs
                    .join_next()
                    .await
                    .expect("registration task exists")??;
            }
        }
        while let Some(result) = registration_jobs.join_next().await {
            result??;
        }
    }
    let execution = execute_registered_query(&session, &args);
    if let Some(seconds) = args.query_timeout {
        tokio::time::timeout(std::time::Duration::from_secs(seconds), execution)
            .await
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("SQL query timed out after {seconds} seconds"),
                )
            })?
    } else {
        execution.await
    }
}

async fn execute_registered_query(
    session: &RustSparkSession,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut union_names = HashSet::new();
    for declaration in &args.unions_by_name {
        let (name, _) = parse_union_by_name(declaration)?;
        if !union_names.insert(name.to_ascii_lowercase()) {
            return Err(format!("Duplicate union destination: {name}").into());
        }
    }
    for declaration in &args.unions_by_name {
        let (name, sources) = parse_union_by_name(declaration)?;
        let union = session.union_tables_by_name(&sources).await?;
        session.register_dataframe(name, union)?;
    }
    let mut view_names = HashSet::new();
    for definition in &args.sql_views {
        let (name, sql) = parse_named_view(definition)?;
        if !view_names.insert(name.to_ascii_lowercase()) {
            return Err(format!("Duplicate SQL view: {name}").into());
        }
        let dataframe = plan_query(session, sql, args).await?;
        session.register_dataframe(name, dataframe)?;
    }
    for definition in &args.materialized_sql_views {
        let (name, sql) = parse_named_view(definition)?;
        if !view_names.insert(name.to_ascii_lowercase()) {
            return Err(format!("Duplicate SQL view: {name}").into());
        }
        let dataframe = plan_query(session, sql, args).await?.cache().await?;
        session.register_dataframe(name, dataframe)?;
    }
    let query = if let Some(sql) = args.sql.as_ref() {
        sql.clone()
    } else if let Some(path) = args.sql_file.as_ref() {
        if path.as_os_str() == "-" {
            read_sql_stdin().await?
        } else {
            std::fs::read_to_string(path)?
        }
    } else {
        unreachable!("clap requires either --sql or --sql-file")
    };
    let mut cached_names = HashSet::new();
    for name in &args.cache_tables {
        if !cached_names.insert(name.to_ascii_lowercase()) {
            return Err(format!("Duplicate cached table: {name}").into());
        }
        session.cache_table(name).await?;
    }
    execute_script(session, &query, args).await
}

// Use a detached reader thread so a blocked stdin read cannot stall the query timer.
async fn read_sql_stdin() -> Result<String, Box<dyn std::error::Error>> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let mut query = String::new();
        let result = std::io::stdin().read_to_string(&mut query).map(|_| query);
        let _ = sender.send(result);
    });
    Ok(receiver.await??)
}

// Parse with DataFusion's dialect to avoid splitting on semicolons in SQL literals
// or comments. Execute setup statements before presenting the final result.
async fn execute_script(
    session: &RustSparkSession,
    script: &str,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    let script = script.strip_prefix('\u{feff}').unwrap_or(script);
    let mut statements = DFParser::parse_sql(script)?;
    let last = statements
        .pop_back()
        .ok_or("SQL script must contain at least one statement")?;
    for statement in statements {
        plan_query(session, &statement.to_string(), args)
            .await?
            .collect()
            .await?;
    }
    execute_query(session, &last.to_string(), args).await
}

async fn plan_query(
    session: &RustSparkSession,
    query: &str,
    args: &Args,
) -> Result<datafusion::prelude::DataFrame, Box<dyn std::error::Error>> {
    let dataframe = if !args.named_params.is_empty() {
        let params: HashMap<_, _> = args.named_params.iter().cloned().collect();
        session.sql_with_params(query, params).await?
    } else if !args.params.is_empty() {
        session.sql_with_params(query, args.params.clone()).await?
    } else {
        session.sql(query).await?
    };
    Ok(dataframe)
}

fn reject_duplicate_output_fields(
    dataframe: &datafusion::prelude::DataFrame,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut names = HashSet::new();
    for field in dataframe.schema().fields() {
        if !names.insert(field.name()) {
            return Err(format!("Duplicate output column: {}", field.name()).into());
        }
    }
    Ok(())
}

fn is_broken_pipe(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut source = Some(error);
    while let Some(current) = source {
        if current
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::BrokenPipe)
        {
            return true;
        }
        // Arrow CSV/JSON writers may flatten I/O errors to string-only ArrowError values.
        if current
            .downcast_ref::<datafusion::arrow::error::ArrowError>()
            .is_some_and(|arrow| {
                let message = arrow.to_string().to_ascii_lowercase();
                message.contains("broken pipe")
                    || message.contains("pipe is being closed")
                    || message.contains("pipe has been ended")
            })
        {
            return true;
        }
        source = current.source();
    }
    false
}

async fn execute_query(
    session: &RustSparkSession,
    query: &str,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    let dataframe = plan_query(session, query, args).await?;
    if args.stdout_format.is_some() || args.output_csv.is_some() || args.output_json.is_some() {
        reject_duplicate_output_fields(&dataframe)?;
    }
    if args.describe {
        for field in dataframe.schema().fields() {
            let nullability = if field.is_nullable() {
                "NULL"
            } else {
                "NOT NULL"
            };
            println!("{}\t{}\t{nullability}", field.name(), field.data_type());
        }
    } else if args.explain {
        dataframe.explain(false, false)?.show().await?;
    } else if args.explain_analyze {
        dataframe.explain(true, false)?.show().await?;
    } else if let Some(path) = args.output_parquet.as_ref() {
        let path = path.to_str().ok_or("Output path must be valid UTF-8")?;
        let writer_options = args.parquet_compression.as_ref().map(|codec| {
            let mut options = TableParquetOptions::default();
            options.global.compression = Some(codec.clone());
            options
        });
        dataframe
            .write_parquet(
                path,
                output_write_options(args.output_single_file),
                writer_options,
            )
            .await?;
    } else if let Some(path) = args.output_csv.as_ref() {
        let path = path.to_str().ok_or("Output path must be valid UTF-8")?;
        dataframe
            .write_csv(path, output_write_options(args.output_single_file), None)
            .await?;
    } else if let Some(path) = args.output_json.as_ref() {
        let path = path.to_str().ok_or("Output path must be valid UTF-8")?;
        dataframe
            .write_json(path, output_write_options(args.output_single_file), None)
            .await?;
    } else if let Some(path) = args.output_arrow.as_ref() {
        write_arrow_ipc(dataframe, path).await?;
    } else if let Some(format) = args.stdout_format {
        let dataframe = if let Some(max_rows) = args.max_rows {
            let offset = args.offset.unwrap_or(0);
            offset
                .checked_add(max_rows)
                .ok_or("--offset plus --max-rows exceeds the supported range")?;
            dataframe.limit(offset, Some(max_rows))?
        } else {
            dataframe
        };
        if let Err(error) = write_stdout(dataframe, format).await
            && !is_broken_pipe(error.as_ref())
        {
            return Err(error);
        }
    } else if args.count_only {
        println!("{}", dataframe.count().await?);
    } else if let Some(max_rows) = args.max_rows {
        if let Some(offset) = args.offset {
            offset
                .checked_add(max_rows)
                .ok_or("--offset plus --max-rows exceeds the supported range")?;
            dataframe
                .limit(offset, Some(max_rows))?
                .show_limit(max_rows)
                .await?;
        } else {
            dataframe.show_limit(max_rows).await?;
        }
    } else {
        dataframe.show().await?;
    }
    Ok(())
}

// Arrow's CSV writer converts I/O errors into strings. Track the original
// BrokenPipe kind before Arrow discards it so shell pipelines exit cleanly.
struct PipeTrackingWriter<W> {
    inner: W,
    broken_pipe: Arc<AtomicBool>,
}

impl<W: Write> Write for PipeTrackingWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let result = self.inner.write(bytes);
        if result
            .as_ref()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::BrokenPipe)
        {
            self.broken_pipe.store(true, Ordering::Relaxed);
        }
        result
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let result = self.inner.flush();
        if result
            .as_ref()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::BrokenPipe)
        {
            self.broken_pipe.store(true, Ordering::Relaxed);
        }
        result
    }
}

fn preserve_writer_pipe_error(
    error: datafusion::arrow::error::ArrowError,
    broken_pipe: &AtomicBool,
) -> Box<dyn std::error::Error> {
    if broken_pipe.load(Ordering::Relaxed) {
        Box::new(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
    } else {
        Box::new(error)
    }
}

// Write one record batch at a time so stdout never requires collecting all results.
async fn write_stdout(
    dataframe: datafusion::prelude::DataFrame,
    format: StdoutFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = dataframe.execute_stream().await?;
    let schema = stream.schema();
    let stdout = std::io::stdout();
    let mut output = std::io::BufWriter::new(stdout.lock());
    let broken_pipe = Arc::new(AtomicBool::new(false));
    let mut tracked = PipeTrackingWriter {
        inner: &mut output,
        broken_pipe: Arc::clone(&broken_pipe),
    };
    match format {
        StdoutFormat::Csv => {
            let mut writer = datafusion::arrow::csv::WriterBuilder::new().build(&mut tracked);
            let mut wrote_batch = false;
            while let Some(batch) = stream.next().await {
                writer
                    .write(&batch?)
                    .map_err(|error| preserve_writer_pipe_error(error, &broken_pipe))?;
                wrote_batch = true;
            }
            if !wrote_batch {
                // Include column headers even when the query returns no rows.
                writer
                    .write(&datafusion::arrow::record_batch::RecordBatch::new_empty(
                        schema,
                    ))
                    .map_err(|error| preserve_writer_pipe_error(error, &broken_pipe))?;
            }
        }
        StdoutFormat::Json => {
            let mut writer = datafusion::arrow::json::writer::WriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, datafusion::arrow::json::writer::LineDelimited>(&mut tracked);
            while let Some(batch) = stream.next().await {
                writer
                    .write(&batch?)
                    .map_err(|error| preserve_writer_pipe_error(error, &broken_pipe))?;
            }
            writer
                .finish()
                .map_err(|error| preserve_writer_pipe_error(error, &broken_pipe))?;
        }
    }
    drop(tracked);
    output.flush()?;
    Ok(())
}

async fn write_arrow_ipc(
    dataframe: datafusion::prelude::DataFrame,
    path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = dataframe.execute_stream().await?;
    let schema = stream.schema();
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let temporary = tempfile::NamedTempFile::new_in(directory)?;
    let mut writer = FileWriter::try_new_buffered(temporary.reopen()?, &schema)?;
    while let Some(batch) = stream.next().await {
        writer.write(&batch?)?;
    }
    writer.finish()?;
    drop(writer);
    temporary.persist(path)?;
    Ok(())
}

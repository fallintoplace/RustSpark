use std::{collections::HashSet, path::PathBuf, sync::Arc};

use clap::{ArgGroup, Parser, ValueEnum};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum StdoutFormat {
    Csv,
    Tsv,
    Json,
    Arrow,
}

#[derive(Parser)]
#[command(
    version,
    about = "Run local SQL queries over CSV, JSON or Parquet files",
    group(ArgGroup::new("input").required(false).multiple(true).args(["csv", "parquet", "json", "csv_tables", "parquet_tables", "json_tables"])),
    group(ArgGroup::new("query").required(true).args(["sql", "sql_file", "interactive"]))
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

    #[arg(long = "union-distinct-by-name", value_name = "NAME=TABLE1,TABLE2,...")]
    unions_distinct_by_name: Vec<String>,

    #[arg(long, value_name = "QUERY")]
    sql: Option<String>,

    #[arg(long, value_name = "PATH")]
    sql_file: Option<PathBuf>,

    /// Load saved Parquet tables from this directory on each run.
    #[arg(long = "database-dir", value_name = "DIR")]
    database_dir: Option<PathBuf>,

    /// Save the final SQL query as a new Parquet table in --database-dir.
    #[arg(
        long = "save-table",
        value_name = "NAME",
        value_parser = parse_table_name,
        requires = "database_dir",
        conflicts_with_all = [
            "describe", "explain", "explain_analyze", "count_only", "max_rows",
            "stdout_format", "output_parquet", "output_csv", "output_json",
            "output_arrow", "output_single_file"
        ]
    )]
    save_table: Option<String>,

    /// Start a SQL shell that executes semicolon-terminated statements.
    #[arg(long)]
    interactive: bool,

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

fn parse_schema_data_type(kind: &str, source: &str) -> Result<DataType, String> {
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
    Ok(data_type)
}

fn parse_column_schema(value: &str, source: &str) -> Result<Schema, String> {
    let mut fields = Vec::new();
    let mut names = HashSet::new();
    for entry in value.split(',') {
        let (name, kind) = entry
            .split_once(':')
            .ok_or_else(|| format!("{source} schema needs NAME:TYPE entries"))?;
        if !valid_identifier(name) {
            return Err(format!(
                "{source} schema column names must be SQL identifiers"
            ));
        }
        if !names.insert(name.to_ascii_lowercase()) {
            return Err(format!("Duplicate {source} schema column: {name}"));
        }
        let data_type = parse_schema_data_type(kind, source)?;
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
        "bool" | "boolean" => {
            if raw.eq_ignore_ascii_case("true") {
                Ok(ScalarValue::Boolean(Some(true)))
            } else if raw.eq_ignore_ascii_case("false") {
                Ok(ScalarValue::Boolean(Some(false)))
            } else {
                Err("--param bool must be true or false".to_owned())
            }
        }
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

mod read_options;
use read_options::{csv_read_options, json_read_options, parquet_read_options};

mod csv_input;
use csv_input::register_csv_with_null_value;

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

mod validation;
use validation::{validate_format_options, validate_inputs};

mod session;
use session::new_session;

mod registration;
use registration::register_inputs;

mod persistence;
use persistence::register_database_tables;

mod query;
use query::execute_script;

mod interactive;
use interactive::run_interactive;

mod setup;
use setup::{cache_tables, read_query, register_derived_tables};

// The database catalog treats subdirectories as stored tables. Exporting a
// CSV/JSON/Parquet directory there can make the next startup fail to load it.
fn reject_database_export_collision(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let Some(root) = args.database_dir.as_deref() else {
        return Ok(());
    };
    let root = root.canonicalize()?;
    for destination in [
        args.output_arrow.as_ref(),
        args.output_parquet.as_ref(),
        args.output_csv.as_ref(),
        args.output_json.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        let normalized = if destination.exists() {
            destination.canonicalize()?
        } else {
            let parent = destination
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| std::path::Path::new("."));
            let name = destination
                .file_name()
                .ok_or("Output path must name a file or directory")?;
            parent.canonicalize()?.join(name)
        };
        if normalized.starts_with(&root) {
            return Err("Output path must not be inside --database-dir".into());
        }
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
    validate_inputs(&args)?;
    // Serialize local catalog reads and writes across RustSpark processes.
    // Keep this file handle alive through interactive sessions and exports.
    let _database_lock = if let Some(root) = args.database_dir.as_deref() {
        Some(persistence::lock_database(root)?)
    } else {
        None
    };
    reject_database_export_collision(&args)?;
    let session = new_session(&args)?;
    validate_format_options(&args)?;
    register_inputs(&session, &args).await?;
    if let Some(directory) = args.database_dir.as_deref() {
        register_database_tables(&session, directory).await?;
    }
    if args.interactive {
        return run_interactive(&session, &args).await;
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
    register_derived_tables(session, args).await?;
    let query = read_query(args).await?;
    cache_tables(session, args).await?;
    execute_script(session, &query, args).await
}

mod output;
use output::{write_arrow_ipc, write_stdout};

use std::{collections::HashSet, io::Read, path::PathBuf, sync::Arc};

use clap::{ArgGroup, Parser};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::{CsvReadOptions, JsonReadOptions, ParquetReadOptions, SessionConfig};
use rustspark::RustSparkSession;

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

    #[arg(long = "csv-schema", value_name = "NAME:TYPE,...", value_parser = parse_csv_schema)]
    csv_schema: Option<Schema>,

    #[arg(long = "csv-schema-rows", value_name = "ROWS", value_parser = parse_csv_schema_rows)]
    csv_schema_rows: Option<usize>,

    #[arg(long, value_name = "PATH")]
    json: Option<PathBuf>,

    #[arg(long = "json-schema-rows", value_name = "ROWS", value_parser = parse_json_schema_rows)]
    json_schema_rows: Option<usize>,

    #[arg(long = "json-array")]
    json_array: bool,

    #[arg(long = "json-table", value_name = "NAME=PATH")]
    json_tables: Vec<String>,

    #[arg(long, default_value = "data", conflicts_with_all = ["csv_tables", "parquet_tables", "json_tables"])]
    table: String,

    #[arg(long = "cache-table", value_name = "NAME")]
    cache_tables: Vec<String>,

    #[arg(long, value_name = "QUERY")]
    sql: Option<String>,

    #[arg(long, value_name = "PATH")]
    sql_file: Option<PathBuf>,

    #[arg(long = "sql-view", value_name = "NAME=SQL")]
    sql_views: Vec<String>,

    #[arg(
        long = "describe",
        conflicts_with_all = ["max_rows", "output_parquet", "output_csv", "output_json"]
    )]
    describe: bool,

    #[arg(
        long = "explain",
        conflicts_with_all = ["describe", "max_rows", "output_parquet", "output_csv", "output_json"]
    )]
    explain: bool,

    #[arg(
        long = "explain-analyze",
        conflicts_with_all = ["describe", "explain", "max_rows", "output_parquet", "output_csv", "output_json"]
    )]
    explain_analyze: bool,

    #[arg(long, value_name = "ROWS", value_parser = parse_positive_rows)]
    max_rows: Option<usize>,

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

fn parse_named_table(value: &str) -> Result<(&str, &str), String> {
    let (name, path) = value
        .split_once('=')
        .ok_or_else(|| "Named table must use NAME=PATH".to_owned())?;
    let valid_first = name
        .bytes()
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_');
    if !valid_first
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        || path.is_empty()
    {
        return Err("Named table needs a SQL identifier and non-empty path".into());
    }
    Ok((name, path))
}

fn parse_named_view(value: &str) -> Result<(&str, &str), String> {
    let (name, sql) = parse_named_table(value)
        .map_err(|_| "--sql-view must use NAME=SQL with a valid SQL identifier".to_owned())?;
    if sql.trim().is_empty() {
        return Err("--sql-view SQL query cannot be empty".into());
    }
    Ok((name, sql))
}

fn parse_csv_schema(value: &str) -> Result<Schema, String> {
    let mut fields = Vec::new();
    let mut names = HashSet::new();
    for entry in value.split(',') {
        let (name, kind) = entry
            .split_once(':')
            .ok_or_else(|| "CSV schema needs NAME:TYPE entries".to_owned())?;
        let valid_first = name
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_');
        if !valid_first
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err("CSV schema column names must be SQL identifiers".into());
        }
        if !names.insert(name.to_ascii_lowercase()) {
            return Err(format!("Duplicate CSV schema column: {name}"));
        }
        let data_type = match kind.to_ascii_lowercase().as_str() {
            "utf8" | "string" => DataType::Utf8,
            "int64" | "bigint" => DataType::Int64,
            "float64" | "double" => DataType::Float64,
            "bool" | "boolean" => DataType::Boolean,
            _ => return Err(format!("Unsupported CSV type: {kind}")),
        };
        fields.push(Field::new(name, data_type, true));
    }
    Ok(Schema::new(fields))
}

fn parse_csv_schema_rows(value: &str) -> Result<usize, String> {
    let count = value
        .parse::<usize>()
        .map_err(|_| "--csv-schema-rows requires a positive integer".to_owned())?;
    if count == 0 {
        return Err("--csv-schema-rows must be greater than zero".into());
    }
    Ok(count)
}

fn parse_json_schema_rows(value: &str) -> Result<usize, String> {
    let count = value
        .parse::<usize>()
        .map_err(|_| "--json-schema-rows requires a positive integer".to_owned())?;
    if count == 0 {
        return Err("--json-schema-rows must be greater than zero".into());
    }
    Ok(count)
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
    if let Some(rows) = args.csv_schema_rows {
        options.schema_infer_max_records(rows)
    } else {
        options
    }
}

fn json_read_options(array: bool, schema_rows: Option<usize>) -> JsonReadOptions<'static> {
    let options = JsonReadOptions::default().newline_delimited(!array);
    if let Some(rows) = schema_rows {
        options.schema_infer_max_records(rows)
    } else {
        options
    }
}

fn parse_batch_size(value: &str) -> Result<usize, String> {
    let count = value
        .parse::<usize>()
        .map_err(|_| "--batch-size requires a positive integer".to_owned())?;
    if count == 0 {
        return Err("--batch-size must be greater than zero".into());
    }
    Ok(count)
}

fn parse_target_partitions(value: &str) -> Result<usize, String> {
    let count = value
        .parse::<usize>()
        .map_err(|_| "--target-partitions requires a positive integer".to_owned())?;
    if count == 0 {
        return Err("--target-partitions must be greater than zero".into());
    }
    Ok(count)
}

fn parse_positive_rows(value: &str) -> Result<usize, String> {
    let count = value
        .parse::<usize>()
        .map_err(|_| "--max-rows requires a positive integer".to_owned())?;
    if count == 0 {
        return Err("--max-rows must be greater than zero".into());
    }
    Ok(count)
}

pub(crate) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = Arc::new(Args::parse());
    let single_inputs = u8::from(args.csv.is_some())
        + u8::from(args.parquet.is_some())
        + u8::from(args.json.is_some());
    let has_named = !args.csv_tables.is_empty()
        || !args.parquet_tables.is_empty()
        || !args.json_tables.is_empty();
    if single_inputs > 1 || (single_inputs > 0 && has_named) {
        return Err("Use one unnamed input or one or more named tables".into());
    }
    let config = SessionConfig::new();
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
    let session = Arc::new(RustSparkSession::with_config(config));
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
    if args.csv_quote.is_some() && args.csv.is_none() && args.csv_tables.is_empty() {
        return Err("--csv-quote requires a CSV input".into());
    }
    if args.csv_quote == Some(args.csv_delimiter.unwrap_or(b',')) {
        return Err("--csv-quote must differ from the CSV delimiter".into());
    }
    if args.csv_escape.is_some() && args.csv.is_none() && args.csv_tables.is_empty() {
        return Err("--csv-escape requires a CSV input".into());
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
    if args.json_schema_rows.is_some() && args.json.is_none() && args.json_tables.is_empty() {
        return Err("--json-schema-rows requires a JSON input".into());
    }
    if args.json_array && args.json.is_none() && args.json_tables.is_empty() {
        return Err("--json-array requires a JSON input".into());
    }
    if let Some(csv) = args.csv.as_ref() {
        let options = csv_read_options(&args);
        session
            .register_csv_with_options(&args.table, csv, options)
            .await?;
    } else if let Some(parquet) = args.parquet.as_ref() {
        let options = ParquetReadOptions::new()
            .file_extension(args.parquet_extension.as_deref().unwrap_or(".parquet"));
        session
            .register_parquet_with_options(&args.table, parquet, options)
            .await?;
    } else if let Some(json) = args.json.as_ref() {
        let options = json_read_options(args.json_array, args.json_schema_rows);
        session
            .register_json_with_options(&args.table, json, options)
            .await?;
    } else {
        let mut names = HashSet::new();
        // CSV schema inference performs file I/O, so overlap it across inputs.
        // Cap the tasks to avoid opening an unbounded number of files at once.
        let mut csv_jobs = tokio::task::JoinSet::new();
        for mapping in &args.csv_tables {
            let (name, path) = parse_named_table(mapping)?;
            if !names.insert(name.to_ascii_lowercase()) {
                return Err(format!("Duplicate table name: {name}").into());
            }
            let session = Arc::clone(&session);
            let args = Arc::clone(&args);
            let name = name.to_owned();
            let path = path.to_owned();
            csv_jobs.spawn(async move {
                session
                    .register_csv_with_options(&name, &path, csv_read_options(&args))
                    .await
            });
            if csv_jobs.len() >= 4 {
                csv_jobs.join_next().await.expect("CSV task exists")??;
            }
        }
        while let Some(result) = csv_jobs.join_next().await {
            result??;
        }
        for mapping in &args.parquet_tables {
            let (name, path) = parse_named_table(mapping)?;
            if !names.insert(name.to_ascii_lowercase()) {
                return Err(format!("Duplicate table name: {name}").into());
            }
            let options = ParquetReadOptions::new()
                .file_extension(args.parquet_extension.as_deref().unwrap_or(".parquet"));
            session
                .register_parquet_with_options(name, path, options)
                .await?;
        }
        for mapping in &args.json_tables {
            let (name, path) = parse_named_table(mapping)?;
            if !names.insert(name.to_ascii_lowercase()) {
                return Err(format!("Duplicate table name: {name}").into());
            }
            let options = json_read_options(args.json_array, args.json_schema_rows);
            session
                .register_json_with_options(name, path, options)
                .await?;
        }
    }
    for definition in &args.sql_views {
        let (name, sql) = parse_named_view(definition)?;
        session.register_sql_view(name, sql).await?;
    }
    let query = if let Some(sql) = args.sql.as_ref() {
        sql.clone()
    } else if let Some(path) = args.sql_file.as_ref() {
        if path.as_os_str() == "-" {
            let mut query = String::new();
            std::io::stdin().read_to_string(&mut query)?;
            query
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
    let dataframe = session.sql(&query).await?;
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
        dataframe
            .write_parquet(path, DataFrameWriteOptions::new(), None)
            .await?;
    } else if let Some(path) = args.output_csv.as_ref() {
        let path = path.to_str().ok_or("Output path must be valid UTF-8")?;
        dataframe
            .write_csv(path, DataFrameWriteOptions::new(), None)
            .await?;
    } else if let Some(path) = args.output_json.as_ref() {
        let path = path.to_str().ok_or("Output path must be valid UTF-8")?;
        dataframe
            .write_json(path, DataFrameWriteOptions::new(), None)
            .await?;
    } else if let Some(max_rows) = args.max_rows {
        dataframe.show_limit(max_rows).await?;
    } else {
        dataframe.show().await?;
    }
    Ok(())
}

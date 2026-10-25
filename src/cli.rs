use std::{collections::HashSet, io::Read, path::PathBuf};

use clap::{ArgGroup, Parser};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::{CsvReadOptions, JsonReadOptions, SessionConfig};
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

    #[arg(long, value_name = "QUERY")]
    sql: Option<String>,

    #[arg(long, value_name = "PATH")]
    sql_file: Option<PathBuf>,

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
        .truncated_rows(args.csv_allow_short_rows);
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
    let args = Args::parse();
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
    let session = RustSparkSession::with_config(config);
    if args.csv_delimiter.is_some() && args.csv.is_none() && args.csv_tables.is_empty() {
        return Err("--csv-delimiter requires --csv".into());
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
        session.register_parquet(&args.table, parquet).await?;
    } else if let Some(json) = args.json.as_ref() {
        let options = json_read_options(args.json_array, args.json_schema_rows);
        session
            .register_json_with_options(&args.table, json, options)
            .await?;
    } else {
        let mut names = HashSet::new();
        for mapping in &args.csv_tables {
            let (name, path) = parse_named_table(mapping)?;
            if !names.insert(name.to_ascii_lowercase()) {
                return Err(format!("Duplicate table name: {name}").into());
            }
            let options = csv_read_options(&args);
            session
                .register_csv_with_options(name, path, options)
                .await?;
        }
        for mapping in &args.parquet_tables {
            let (name, path) = parse_named_table(mapping)?;
            if !names.insert(name.to_ascii_lowercase()) {
                return Err(format!("Duplicate table name: {name}").into());
            }
            session.register_parquet(name, path).await?;
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
    let query = if let Some(sql) = args.sql {
        sql
    } else if let Some(path) = args.sql_file {
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
    let dataframe = session.sql(&query).await?;
    if let Some(path) = args.output_parquet {
        let path = path.to_str().ok_or("Output path must be valid UTF-8")?;
        dataframe
            .write_parquet(path, DataFrameWriteOptions::new(), None)
            .await?;
    } else if let Some(path) = args.output_csv {
        let path = path.to_str().ok_or("Output path must be valid UTF-8")?;
        dataframe
            .write_csv(path, DataFrameWriteOptions::new(), None)
            .await?;
    } else if let Some(path) = args.output_json {
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

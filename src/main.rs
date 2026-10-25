use std::path::PathBuf;

use clap::{ArgGroup, Parser};
use datafusion::prelude::CsvReadOptions;
use rustspark::RustSparkSession;

#[derive(Parser)]
#[command(
    version,
    about = "Run local SQL queries over CSV, JSON or Parquet files",
    group(ArgGroup::new("input").required(true).args(["csv", "parquet", "json", "csv_tables"])),
    group(ArgGroup::new("query").required(true).args(["sql", "sql_file"]))
)]
struct Args {
    #[arg(long, value_name = "PATH")]
    csv: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    parquet: Option<PathBuf>,

    #[arg(long = "csv-table", value_name = "NAME=PATH")]
    csv_tables: Vec<String>,

    #[arg(long, value_name = "CHAR", value_parser = parse_delimiter)]
    csv_delimiter: Option<u8>,

    #[arg(long, value_name = "PATH")]
    json: Option<PathBuf>,

    #[arg(long, default_value = "data", conflicts_with = "csv_tables")]
    table: String,

    #[arg(long, value_name = "QUERY")]
    sql: Option<String>,

    #[arg(long, value_name = "PATH")]
    sql_file: Option<PathBuf>,

    #[arg(long, value_name = "ROWS", value_parser = parse_positive_rows)]
    max_rows: Option<usize>,
}

fn parse_delimiter(value: &str) -> Result<u8, String> {
    match value.as_bytes() {
        [byte] if *byte != b'\n' && *byte != b'\r' && *byte != b'"' => Ok(*byte),
        _ => Err("CSV delimiter must be one ASCII character other than a quote or newline".into()),
    }
}

fn parse_named_csv(value: &str) -> Result<(&str, &str), String> {
    let (name, path) = value
        .split_once('=')
        .ok_or_else(|| "CSV table must use NAME=PATH".to_owned())?;
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
        return Err("CSV table needs a SQL identifier and non-empty path".into());
    }
    Ok((name, path))
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let session = RustSparkSession::new();
    if args.csv_delimiter.is_some() && args.csv.is_none() && args.csv_tables.is_empty() {
        return Err("--csv-delimiter requires --csv".into());
    }
    if let Some(csv) = args.csv {
        let options = CsvReadOptions::new().delimiter(args.csv_delimiter.unwrap_or(b','));
        session
            .register_csv_with_options(&args.table, csv, options)
            .await?;
    } else if let Some(parquet) = args.parquet {
        session.register_parquet(&args.table, parquet).await?;
    } else if let Some(json) = args.json {
        session.register_json(&args.table, json).await?;
    } else {
        for mapping in &args.csv_tables {
            let (name, path) = parse_named_csv(mapping)?;
            let options = CsvReadOptions::new().delimiter(args.csv_delimiter.unwrap_or(b','));
            session
                .register_csv_with_options(name, path, options)
                .await?;
        }
    }
    let query = if let Some(sql) = args.sql {
        sql
    } else if let Some(path) = args.sql_file {
        std::fs::read_to_string(path)?
    } else {
        unreachable!("clap requires either --sql or --sql-file")
    };
    let dataframe = session.sql(&query).await?;
    if let Some(max_rows) = args.max_rows {
        dataframe.show_limit(max_rows).await?;
    } else {
        dataframe.show().await?;
    }
    Ok(())
}

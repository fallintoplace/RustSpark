use std::path::PathBuf;

use clap::{ArgGroup, Parser};
use datafusion::prelude::CsvReadOptions;
use rustspark::RustSparkSession;

#[derive(Parser)]
#[command(
    version,
    about = "Run local SQL queries over CSV, JSON or Parquet files",
    group(ArgGroup::new("input").required(true).args(["csv", "parquet", "json"])),
    group(ArgGroup::new("query").required(true).args(["sql", "sql_file"]))
)]
struct Args {
    #[arg(long, value_name = "PATH")]
    csv: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    parquet: Option<PathBuf>,

    #[arg(long, value_name = "CHAR", value_parser = parse_delimiter)]
    csv_delimiter: Option<u8>,

    #[arg(long, value_name = "PATH")]
    json: Option<PathBuf>,

    #[arg(long, default_value = "data")]
    table: String,

    #[arg(long, value_name = "QUERY")]
    sql: Option<String>,

    #[arg(long, value_name = "PATH")]
    sql_file: Option<PathBuf>,
}

fn parse_delimiter(value: &str) -> Result<u8, String> {
    match value.as_bytes() {
        [byte] if *byte != b'\n' && *byte != b'\r' && *byte != b'"' => Ok(*byte),
        _ => Err("CSV delimiter must be one ASCII character other than a quote or newline".into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let session = RustSparkSession::new();
    if args.csv_delimiter.is_some() && args.csv.is_none() {
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
    }
    let query = if let Some(sql) = args.sql {
        sql
    } else if let Some(path) = args.sql_file {
        std::fs::read_to_string(path)?
    } else {
        unreachable!("clap requires either --sql or --sql-file")
    };
    session.sql(&query).await?.show().await?;
    Ok(())
}

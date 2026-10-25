use std::path::PathBuf;

use clap::{ArgGroup, Parser};
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

    #[arg(long, value_name = "PATH")]
    json: Option<PathBuf>,

    #[arg(long, default_value = "data")]
    table: String,

    #[arg(long, value_name = "QUERY")]
    sql: Option<String>,

    #[arg(long, value_name = "PATH")]
    sql_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let session = RustSparkSession::new();
    if let Some(csv) = args.csv {
        session.register_csv(&args.table, csv).await?;
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

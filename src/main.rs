use std::path::PathBuf;

use clap::{ArgGroup, Parser};
use rustspark::RustSparkSession;

#[derive(Parser)]
#[command(
    version,
    about = "Run local SQL queries over CSV or Parquet files",
    group(ArgGroup::new("input").required(true).args(["csv", "parquet"]))
)]
struct Args {
    #[arg(long, value_name = "PATH")]
    csv: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    parquet: Option<PathBuf>,

    #[arg(long, default_value = "data")]
    table: String,

    #[arg(long, value_name = "QUERY")]
    sql: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let session = RustSparkSession::new();
    if let Some(csv) = args.csv {
        session.register_csv(&args.table, csv).await?;
    } else if let Some(parquet) = args.parquet {
        session.register_parquet(&args.table, parquet).await?;
    }
    session.sql(&args.sql).await?.show().await?;
    Ok(())
}

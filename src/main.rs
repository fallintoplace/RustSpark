use std::path::PathBuf;

use clap::Parser;
use rustspark::RustSparkSession;

#[derive(Parser)]
#[command(version, about = "Run local SQL queries over CSV files")]
struct Args {
    #[arg(long, value_name = "PATH")]
    csv: PathBuf,

    #[arg(long, default_value = "data")]
    table: String,

    #[arg(long, value_name = "QUERY")]
    sql: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let session = RustSparkSession::new();
    session.register_csv(&args.table, &args.csv).await?;
    session.sql(&args.sql).await?.show().await?;
    Ok(())
}

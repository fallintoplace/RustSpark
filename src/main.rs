use std::{io, path::PathBuf};

use clap::Parser;
use rustspark::RustSparkSession;

#[derive(Parser)]
#[command(version, about = "Run local SQL queries over CSV files")]
struct Args {
    /// Path to a local CSV file (first row contains column names).
    #[arg(long, value_name = "PATH")]
    csv: PathBuf,

    /// SQL table name used for the CSV file.
    #[arg(long, default_value = "data")]
    table: String,

    /// SQL statement to run against the registered table.
    #[arg(long, value_name = "QUERY")]
    sql: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let path = args.csv.to_str().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "CSV path is not valid UTF-8")
    })?;

    let session = RustSparkSession::new();
    session.register_csv(&args.table, path).await?;
    session.sql(&args.sql).await?.show().await?;
    Ok(())
}

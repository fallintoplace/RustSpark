//! Optional Ballista client demo: uses real remote scheduler/executor processes.
//! Run from the RustSpark repository root after starting Ballista 55.
use rustspark::RustSparkSession;

mod scheduler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("BALLISTA_URL")
        .unwrap_or_else(|_| "df://localhost:50050".to_owned());
    let query = std::env::var("RUSTSPARK_SQL")
        .unwrap_or_else(|_| "SELECT COUNT(*) AS n FROM (VALUES (1), (2), (3)) AS input(id)".to_owned());

    let context = scheduler::connect(&url).await?;
    let session = RustSparkSession::from_context(context);

    // The worker must be able to read the same path. Use shared object storage
    // for real clusters; this optional path is intended for a single-host test.
    if let Ok(path) = std::env::var("RUSTSPARK_PARQUET") {
        session.register_parquet("data", &path).await?;
    }

    session.sql(&query).await?.show().await?;
    Ok(())
}

//! Embedded RustSpark example. Run: cargo run --example embedded_csv
use rustspark::RustSparkSession;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let file = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/sales.csv");
    session.register_csv("sales", file).await?;
    let batches = session
        .sql("SELECT city, SUM(amount) AS total FROM sales GROUP BY city ORDER BY city")
        .await?
        .collect()
        .await?;
    let count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    println!("Aggregated {count} cities");
    for batch in batches {
        println!("{batch:?}");
    }
    Ok(())
}

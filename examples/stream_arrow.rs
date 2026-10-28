//! Streaming example. Run: cargo run --example stream_arrow
use futures::StreamExt;
use rustspark::RustSparkSession;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let file = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/sales.csv");
    session.register_csv("sales", file).await?;
    let mut stream = session.sql_stream("SELECT city, amount FROM sales").await?;
    let mut batches = 0usize;
    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        batches += 1;
        rows += batch.num_rows();
    }
    println!("Read {rows} rows in {batches} Arrow batches without collecting them");
    Ok(())
}

//! Arrow in-memory example. Run: cargo run --example in_memory_arrow
use std::sync::Arc;

use datafusion::arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use rustspark::RustSparkSession;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("revenue", DataType::Int64, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["eu", "us", "eu"])),
        Arc::new(Int64Array::from(vec![10, 20, 30])),
    ];
    let batch = RecordBatch::try_new(schema, columns)?;
    let session = RustSparkSession::new();
    session.register_batches("sales", [batch])?;
    let results = session
        .sql("SELECT region, SUM(revenue) AS total FROM sales GROUP BY region ORDER BY region")
        .await?
        .collect()
        .await?;
    for batch in results {
        println!("{batch:?}");
    }
    Ok(())
}

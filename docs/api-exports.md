# Query exports from the Rust API

Use `RustSparkSession::export_sql` to run a read-only query and write its
results to CSV, newline-delimited JSON, or Parquet.

```rust
use rustspark::{RustSparkSession, SqlExportFormat};

# async fn example() -> datafusion::error::Result<()> {
let session = RustSparkSession::new();
session
    .export_sql("SELECT 42 AS answer", "/tmp/rustspark-results", SqlExportFormat::Parquet)
    .await?;
# Ok(())
# }
```

The destination is a **directory**, not a single file. DataFusion writes
one or more part files inside it and does not collect all rows in memory.
Only read-only SQL is allowed, but this is not a security sandbox.
Exports are not atomic. On errors or cancellation a partial directory may remain.
Use the CLI's `--output-single-file` option if you need an atomic single-file
export for supported output formats.

## Atomic directory publication

The Rust `export_sql` API stages CSV, JSON, or Parquet results in a temporary
sibling directory and publishes them by renaming after a complete write.
Existing destinations (including symlinks) are rejected. If SQL planning or
execution fails, or the future is cancelled before publication, the staging
directory is cleaned up. This prevents readers from seeing partial part files.
The final rename requires a destination on the same filesystem, and does not
coordinate with external processes racing to create the same directory. This
is not an fsync durability guarantee.

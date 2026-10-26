use rustspark::{RustSparkSession, SqlExportFormat};

#[tokio::test]
async fn exports_read_only_sql_in_three_file_formats() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_csv(
            "sales",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
        )
        .await?;
    let temporary = tempfile::tempdir()?;
    let sql = "SELECT city, amount FROM sales ORDER BY amount";

    let csv = temporary.path().join("csv");
    session.export_sql(sql, &csv, SqlExportFormat::Csv).await?;
    assert_eq!(session.read_csv(&csv).await?.count().await?, 3);

    let json = temporary.path().join("json");
    session
        .export_sql(sql, &json, SqlExportFormat::Json)
        .await?;
    assert_eq!(session.read_json(&json).await?.count().await?, 3);

    let parquet = temporary.path().join("parquet");
    session
        .export_sql(sql, &parquet, SqlExportFormat::Parquet)
        .await?;
    assert_eq!(session.read_parquet(&parquet).await?.count().await?, 3);
    Ok(())
}

#[tokio::test]
async fn export_rejects_writes_and_keeps_destination_absent()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("must-not-exist");
    assert!(
        session
            .export_sql(
                "CREATE TABLE unsafe_table AS SELECT 1",
                &destination,
                SqlExportFormat::Parquet,
            )
            .await
            .is_err()
    );
    assert!(!destination.exists());
    assert!(!session.table_exists("unsafe_table")?);
    Ok(())
}

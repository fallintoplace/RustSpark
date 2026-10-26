use std::{fs, path::PathBuf};

use datafusion::arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
};
use datafusion::prelude::CsvReadOptions;
use rustspark::RustSparkSession;

#[tokio::test]
async fn multi_file_csv_respects_shared_schema_and_delimiter()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let first = dir.path().join("first.csv");
    let second = dir.path().join("second.csv");
    fs::write(&first, "city;amount\nBerlin;10\n")?;
    fs::write(&second, "city;amount\nMunich;20\n")?;
    let files = [first, second];
    let schema = Schema::new(vec![
        Field::new("city", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
    ]);
    let session = RustSparkSession::new();
    assert_eq!(
        session
            .read_csv_files_with_options(
                &files,
                CsvReadOptions::new().delimiter(b';').schema(&schema),
            )
            .await?
            .count()
            .await?,
        2
    );
    session
        .register_csv_files_with_options(
            "sales",
            &files,
            CsvReadOptions::new().delimiter(b';').schema(&schema),
        )
        .await?;
    let batches = session
        .sql("SELECT SUM(amount) AS total FROM sales")
        .await?
        .collect()
        .await?;
    let total = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(total.value(0), 30);
    assert!(
        session
            .read_csv_files_with_options::<PathBuf>(&[], CsvReadOptions::new())
            .await
            .is_err()
    );
    assert!(
        session
            .register_csv_files_with_options::<PathBuf>("empty", &[], CsvReadOptions::new())
            .await
            .is_err()
    );
    Ok(())
}

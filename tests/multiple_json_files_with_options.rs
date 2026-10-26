use std::{fs, path::PathBuf};

use datafusion::arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
};
use datafusion::prelude::JsonReadOptions;
use rustspark::RustSparkSession;

#[tokio::test]
async fn multi_file_json_supports_array_format_and_explicit_schema() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let first = dir.path().join("first.json");
    let second = dir.path().join("second.json");
    fs::write(&first, "[{\"city\":\"Berlin\",\"amount\":10}]")?;
    fs::write(&second, "[{\"city\":\"Munich\",\"amount\":20}]")?;
    let files = [first, second];
    let schema = Schema::new(vec![
        Field::new("city", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
    ]);
    let session = RustSparkSession::new();
    assert_eq!(
        session
            .read_json_files_with_options(
                &files,
                JsonReadOptions::default().newline_delimited(false).schema(&schema),
            )
            .await?
            .count()
            .await?,
        2
    );
    session
        .register_json_files_with_options(
            "sales",
            &files,
            JsonReadOptions::default().newline_delimited(false).schema(&schema),
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
            .read_json_files_with_options::<PathBuf>(&[], JsonReadOptions::default())
            .await
            .is_err()
    );
    assert!(
        session
            .register_json_files_with_options::<PathBuf>("empty", &[], JsonReadOptions::default())
            .await
            .is_err()
    );
    Ok(())
}

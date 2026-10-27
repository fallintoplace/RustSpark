use std::{fs::File, process::Command, sync::Arc};

use datafusion::{
    arrow::{
        array::{BinaryArray, Int64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    parquet::arrow::ArrowWriter,
};

#[test]
fn parquet_binary_as_string_can_be_enabled_for_clickbench_columns()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("hits.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("SearchPhrase", DataType::Binary, false),
        Field::new("UserID", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BinaryArray::from(vec![&b"hello"[..], &b"world"[..]])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )?;
    let mut writer = ArrowWriter::try_new(File::create(&path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--parquet")
        .arg(&path)
        .args([
            "--parquet-binary-as-string",
            "--sql",
            "SELECT COUNT(*) AS n FROM data WHERE \"SearchPhrase\" = 'hello'",
            "--stdout-format",
            "json",
        ])
        .output()?;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8(output.stdout)?.trim(), r#"{"n":1}"#);
    Ok(())
}

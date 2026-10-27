use std::{
    fs::File,
    path::Path,
    process::Command,
    sync::Arc,
};

use datafusion::{
    arrow::{
        array::{BinaryArray, Int32Array, Int64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    parquet::arrow::ArrowWriter,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn create_fixture(path: &Path) -> TestResult {
    let schema = Arc::new(Schema::new(vec![
        Field::new("SearchPhrase", DataType::Binary, false),
        Field::new("UserID", DataType::Int64, false),
        Field::new("EventDate", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BinaryArray::from(vec![
                &b"alpha"[..], &b"alpha"[..], &b"beta"[..], &b""[..],
            ])),
            Arc::new(Int64Array::from(vec![1, 2, 1, 3])),
            Arc::new(Int32Array::from(vec![15887, 15900, 15917, 15918])),
        ],
    )?;
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn query(path: &Path, sql: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--parquet")
        .arg(path)
        .args([
            "--table",
            "hits_raw",
            "--parquet-binary-as-string",
            "--sql-view",
            r#"hits=SELECT * EXCEPT ("EventDate"), CAST(CAST("EventDate" AS INTEGER) AS DATE) AS "EventDate" FROM hits_raw"#,
            "--sql",
            sql,
            "--stdout-format",
            "json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "query: {sql}\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

#[test]
fn parquet_string_columns_and_date_view_support_analytics_queries() -> TestResult {
    let dir = tempfile::tempdir()?;
    let input = dir.path().join("hits.parquet");
    create_fixture(&input)?;

    let count = query(&input, "SELECT COUNT(*) AS n FROM hits;")?;
    assert_eq!(count.trim(), r#"{"n":4}"#);

    let grouped = query(
        &input,
        r#"SELECT "SearchPhrase", COUNT(*) AS n FROM hits
            WHERE "SearchPhrase" <> ''
            GROUP BY "SearchPhrase"
            ORDER BY "SearchPhrase";"#,
    )?;
    let grouped_rows: Vec<_> = grouped.lines().collect();
    assert_eq!(
        grouped_rows,
        vec![r#"{"SearchPhrase":"alpha","n":2}"#, r#"{"SearchPhrase":"beta","n":1}"#]
    );

    let dates = query(
        &input,
        r#"SELECT COUNT(*) AS n FROM hits
            WHERE "EventDate" >= '2013-07-01'
              AND "EventDate" <= '2013-07-31';"#,
    )?;
    assert_eq!(dates.trim(), r#"{"n":3}"#);

    let distinct = query(&input, r#"SELECT COUNT(DISTINCT "UserID") AS n FROM hits;"#)?;
    assert_eq!(distinct.trim(), r#"{"n":3}"#);
    Ok(())
}

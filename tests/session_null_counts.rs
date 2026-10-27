use std::sync::Arc;

use datafusion::arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn nullable_batch() -> TestResultBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("quoted\"column", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some("A"),
                None,
                Some("B"),
                Some("C"),
            ])),
            Arc::new(Int64Array::from(vec![Some(1), None, None, Some(4)])),
        ],
    )
}

type TestResultBatch = datafusion::arrow::error::Result<RecordBatch>;

#[tokio::test]
async fn counts_nulls_by_column_and_escapes_quoted_names() -> TestResult {
    let session = RustSparkSession::new();
    session.register_batches("events", [nullable_batch()?])?;

    let result = session.null_counts("events", &[]).await?.collect().await?;
    assert_eq!(result.len(), 1);
    let batch = &result[0];
    assert_eq!(batch.num_rows(), 1);
    for (index, (name, expected)) in [("name", 1), ("quoted\"column", 2)].into_iter().enumerate() {
        assert_eq!(batch.schema().field(index).name(), name);
        let values = batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), expected);
    }

    let selected = session
        .null_counts("events", &["quoted\"column"])
        .await?
        .collect()
        .await?;
    assert_eq!(selected[0].num_columns(), 1);
    assert_eq!(selected[0].schema().field(0).name(), "quoted\"column");
    assert_eq!(
        selected[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
    Ok(())
}

#[tokio::test]
async fn null_counts_validate_columns_and_count_empty_tables() -> TestResult {
    let session = RustSparkSession::new();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    session.register_batches("empty", [RecordBatch::new_empty(schema)])?;

    let results = session.null_counts("empty", &[]).await?.collect().await?;
    let counts = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.value(0), 0);
    assert!(session.null_counts("empty", &["missing"]).await.is_err());
    assert!(
        session
            .null_counts("empty", &["value", "VALUE"])
            .await
            .is_err()
    );
    assert!(session.null_counts("missing", &[]).await.is_err());
    Ok(())
}

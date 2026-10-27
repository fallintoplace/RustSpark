use std::sync::Arc;

use datafusion::arrow::{
    array::{Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn distinct_column_values_are_lazy_and_unique() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_csv("sales", "tests/fixtures/sales.csv")
        .await?;

    let batches = session
        .distinct_column_values("sales", "city")
        .await?
        .collect()
        .await?;
    let mut values = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            values.push(column.value(index).to_owned());
        }
    }
    values.sort();
    assert_eq!(values, ["Berlin", "Munich"]);
    assert!(
        session
            .distinct_column_values("sales", "missing")
            .await
            .is_err()
    );
    assert!(
        session
            .distinct_column_values("missing", "city")
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn distinct_column_values_keep_only_one_null() -> TestResult {
    let session = RustSparkSession::new();
    let schema = Arc::new(Schema::new(vec![Field::new("city", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![
            Some("A"),
            None,
            Some("A"),
            None,
        ]))],
    )?;
    session.register_batches("events", [batch])?;

    let batches = session
        .distinct_column_values("events", "city")
        .await?
        .collect()
        .await?;
    let mut values = Vec::new();
    let mut nulls = 0;
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            if column.is_null(index) {
                nulls += 1;
            } else {
                values.push(column.value(index).to_owned());
            }
        }
    }
    assert_eq!(values, ["A"]);
    assert_eq!(nulls, 1);
    Ok(())
}

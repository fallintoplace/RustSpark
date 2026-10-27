use datafusion::{arrow::array::Int64Array, common::ScalarValue};
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn wide_fillna_groups_repeated_typed_values() -> TestResult {
    let session = RustSparkSession::new();
    let projection = (0..24)
        .map(|index| format!("CAST(NULL AS BIGINT) AS c{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    session
        .register_sql_view("wide", &format!("SELECT {projection}"))
        .await?;
    let replacements = (0..24)
        .map(|index| (format!("c{index}"), ScalarValue::Int64(Some(index % 9))))
        .collect::<Vec<_>>();
    let borrowed = replacements
        .iter()
        .map(|(name, value)| (name.as_str(), value.clone()))
        .collect::<Vec<_>>();
    let output = session
        .fill_nulls("wide", &borrowed)
        .await?
        .collect()
        .await?;
    assert_eq!(output.len(), 1);
    for index in 0..24 {
        let values = output[0]
            .column(index as usize)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("BIGINT output");
        assert_eq!(values.value(0), index % 9);
    }
    Ok(())
}

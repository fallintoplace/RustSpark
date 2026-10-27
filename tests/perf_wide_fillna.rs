use datafusion::{arrow::{array::Int64Array, array::Array}, common::ScalarValue};
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

#[tokio::test]
async fn sparse_fillna_on_wide_schema_preserves_other_columns() -> TestResult {
    let session = RustSparkSession::new();
    let projection = (0..64)
        .map(|index| format!("CAST(NULL AS BIGINT) AS c{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    session
        .register_sql_view("wide_sparse", &format!("SELECT {projection}"))
        .await?;
    let result = session
        .fill_nulls("wide_sparse", &[("c63", ScalarValue::Int64(Some(7)))])
        .await?
        .collect()
        .await?;
    assert_eq!(result[0].num_columns(), 64);
    let first = result[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let last = result[0].column(63).as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(first.is_null(0));
    assert_eq!(last.value(0), 7);
    Ok(())
}

use datafusion::arrow::array::BooleanArray;
use rustspark::RustSparkSession;

async fn assert_true_columns(sql: &str) -> Result<(), Box<dyn std::error::Error>> {
    let batches = RustSparkSession::new().sql(sql).await?.collect().await?;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    for column in batches[0].columns() {
        let value = column
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("expected a boolean expression");
        assert_eq!(value.iter().collect::<Vec<_>>(), vec![Some(true)]);
    }
    Ok(())
}

#[tokio::test]
async fn constructs_and_queries_arrays() -> Result<(), Box<dyn std::error::Error>> {
    assert_true_columns(
        "SELECT ARRAY_LENGTH(MAKE_ARRAY(10, 20, 30)) = 3 AS correct_length, \
                ARRAY_HAS(MAKE_ARRAY(10, 20, 30), 20) AS contains_item",
    )
    .await
}

#[tokio::test]
async fn constructs_and_queries_maps() -> Result<(), Box<dyn std::error::Error>> {
    assert_true_columns(
        "SELECT ARRAY_LENGTH(MAP_KEYS(MAP(['a', 'b'], [10, 20]))) = 2 AS has_keys, \
                ARRAY_LENGTH(MAP_VALUES(MAP(['a', 'b'], [10, 20]))) = 2 AS has_values",
    )
    .await
}

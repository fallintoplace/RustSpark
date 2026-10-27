use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn repeated_in_literals_keep_sql_null_semantics() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "numbers",
            "SELECT id FROM (VALUES (1), (3), (CAST(NULL AS BIGINT))) AS t(id)",
        )
        .await?;

    let mut literals = vec![ScalarValue::Int64(Some(1)); 32];
    literals.extend([
        ScalarValue::Int64(Some(3)),
        ScalarValue::Int64(None),
        ScalarValue::Int64(None),
    ]);
    assert_eq!(
        session
            .filter_rows_in_values("numbers", "id", &literals)
            .await?
            .count()
            .await?,
        2
    );
    assert_eq!(
        session
            .filter_rows_not_in_values("numbers", "id", &literals)
            .await?
            .count()
            .await?,
        0
    );
    let only_ones = vec![ScalarValue::Int64(Some(1)); 32];
    assert_eq!(
        session
            .filter_rows_not_in_values("numbers", "id", &only_ones)
            .await?
            .count()
            .await?,
        1
    );
    Ok(())
}

use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn returns_first_bound_read_only_row_including_nulls()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let params = || vec![ScalarValue::Int64(Some(9))];
    let row = session
        .sql_read_only_first_row_with_params(
            "SELECT CAST($1 AS BIGINT), CAST(NULL AS BIGINT)",
            params(),
        )
        .await?;
    assert_eq!(
        row,
        Some(vec![ScalarValue::Int64(Some(9)), ScalarValue::Int64(None)])
    );

    assert_eq!(
        session
            .sql_read_only_first_row_with_params("SELECT CAST($1 AS BIGINT) WHERE FALSE", params(),)
            .await?,
        None
    );
    assert!(
        session
            .sql_read_only_first_row_with_params(
                "CREATE TABLE denied AS SELECT 1",
                Vec::<ScalarValue>::new(),
            )
            .await
            .is_err()
    );
    assert!(!session.table_exists("denied")?);
    Ok(())
}

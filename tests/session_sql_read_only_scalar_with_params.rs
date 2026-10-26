use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn extracts_only_one_bound_scalar_and_rejects_writes() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let params = || vec![ScalarValue::Int64(Some(23))];
    assert_eq!(
        session
            .sql_read_only_scalar_with_params("SELECT CAST($1 AS BIGINT)", params())
            .await?,
        ScalarValue::Int64(Some(23))
    );
    assert!(
        session
            .sql_read_only_scalar_with_params("SELECT $1, 2", params())
            .await
            .is_err()
    );
    assert!(
        session
            .sql_read_only_scalar_with_params("SELECT $1 WHERE FALSE", params())
            .await
            .is_err()
    );
    assert!(
        session
            .sql_read_only_scalar_with_params(
                "CREATE TABLE denied AS SELECT 1",
                Vec::<ScalarValue>::new(),
            )
            .await
            .is_err()
    );
    assert!(!session.table_exists("denied")?);
    Ok(())
}

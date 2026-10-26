use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn reads_one_bound_scalar_and_rejects_invalid_shapes() -> Result<(), Box<dyn std::error::Error>>
{
    let session = RustSparkSession::new();
    let params = || vec![ScalarValue::Int64(Some(12))];
    assert_eq!(
        session
            .sql_scalar_with_params("SELECT CAST($1 AS BIGINT)", params())
            .await?,
        ScalarValue::Int64(Some(12))
    );
    assert!(
        session
            .sql_scalar_with_params("SELECT $1, 2", params())
            .await
            .is_err()
    );
    assert!(
        session
            .sql_scalar_with_params("SELECT $1 WHERE FALSE", params())
            .await
            .is_err()
    );
    assert!(
        session
            .sql_scalar_with_params("SELECT $1 UNION ALL SELECT 7", params())
            .await
            .is_err()
    );
    Ok(())
}

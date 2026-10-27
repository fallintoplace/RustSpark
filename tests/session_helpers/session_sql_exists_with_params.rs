use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn checks_for_bound_rows_without_executing_writes() -> Result<(), Box<dyn std::error::Error>>
{
    let session = RustSparkSession::new();
    let query = "SELECT 1 WHERE 1 = $1";
    assert!(
        session
            .sql_exists_with_params(query, vec![ScalarValue::Int64(Some(1))])
            .await?
    );
    assert!(
        !session
            .sql_exists_with_params(query, vec![ScalarValue::Int64(Some(2))])
            .await?
    );
    assert!(
        session
            .sql_exists_with_params("CREATE TABLE denied AS SELECT 1", Vec::<ScalarValue>::new(),)
            .await
            .is_err()
    );
    assert!(!session.table_exists("denied")?);
    Ok(())
}

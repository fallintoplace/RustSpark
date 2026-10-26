use datafusion::prelude::SessionContext;
use rustspark::RustSparkSession;

#[tokio::test]
async fn external_datafusion_context_runs_sql() -> datafusion::error::Result<()> {
    let context = SessionContext::new();
    let session = RustSparkSession::from_context(context);
    let result = session.sql_scalar("SELECT CAST(42 AS BIGINT)").await?;
    assert_eq!(result.to_string(), "42");
    Ok(())
}

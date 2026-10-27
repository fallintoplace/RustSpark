use datafusion::prelude::{col, lit};
use rustspark::RustSparkSession;

#[tokio::test]
async fn selects_computed_expressions_and_preserves_source()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "items",
            "SELECT CAST(4 AS BIGINT) AS amount, 'Berlin' AS city",
        )
        .await?;

    let selected = session
        .select_expressions(
            "items",
            &[col("city"), (col("amount") + lit(2_i64)).alias("adjusted")],
        )
        .await?;
    session.register_dataframe("projection", selected)?;
    assert_eq!(
        session
            .sql_scalar("SELECT adjusted FROM projection")
            .await?
            .to_string(),
        "6"
    );
    assert_eq!(session.describe_table("projection").await?.len(), 2);
    assert_eq!(session.describe_table("items").await?.len(), 2);
    assert!(session.select_expressions("items", &[]).await.is_err());
    assert!(
        session
            .select_expressions("items", &[col("missing")])
            .await
            .is_err()
    );
    Ok(())
}

use datafusion::{
    common::ScalarValue,
    prelude::{col, lit},
};
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn with_columns_evaluates_updates_against_original_input() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "numbers",
            "SELECT CAST(1 AS BIGINT) AS a, CAST(2 AS BIGINT) AS b",
        )
        .await?;

    let updated = session
        .with_columns(
            "numbers",
            &[("a", col("a") + lit(10i64)), ("total", col("a") + col("b"))],
        )
        .await?;
    let names: Vec<_> = updated
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(names, ["a", "b", "total"]);

    session.register_dataframe("updated", updated)?;
    assert_eq!(
        session
            .sql_scalar("SELECT total FROM updated WHERE a = 11 AND b = 2")
            .await?,
        ScalarValue::Int64(Some(3))
    );
    assert_eq!(
        session.sql_scalar("SELECT a FROM numbers").await?,
        ScalarValue::Int64(Some(1))
    );

    assert_eq!(
        session.with_columns("numbers", &[]).await?.count().await?,
        1
    );
    assert!(
        session
            .with_columns("numbers", &[("a", lit(1i64)), ("A", lit(2i64))])
            .await
            .is_err()
    );
    assert!(
        session
            .with_columns("numbers", &[(" ", lit(1i64))])
            .await
            .is_err()
    );
    assert!(
        session
            .with_columns("numbers", &[("broken", col("not_in_input"))])
            .await
            .is_err()
    );
    Ok(())
}

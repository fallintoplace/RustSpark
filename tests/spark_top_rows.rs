use datafusion::arrow::array::Int64Array;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn top_rows_selects_ordered_rows_with_tie_breakers() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "items",
            "SELECT 3 AS id, 2 AS score
             UNION ALL SELECT 1, 3
             UNION ALL SELECT 2, 3
             UNION ALL SELECT 4, 1",
        )
        .await?;

    let top = session
        .top_rows("items", &[("score", false), ("id", true)], 2)
        .await?;
    let batches = top.collect().await?;
    let mut actual = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("expected an Int64 id column")?;
        for row in 0..batch.num_rows() {
            actual.push(ids.value(row));
        }
    }
    assert_eq!(actual, vec![1, 2]);
    assert_eq!(session.table("items").await?.count().await?, 4);
    Ok(())
}

#[tokio::test]
async fn top_rows_handles_empty_limits_and_invalid_order() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view("items", "SELECT 1 AS id UNION ALL SELECT 2")
        .await?;

    assert_eq!(
        session
            .top_rows("items", &[("id", true)], 0)
            .await?
            .count()
            .await?,
        0
    );
    assert_eq!(
        session
            .top_rows("items", &[("id", true)], 10)
            .await?
            .count()
            .await?,
        2
    );
    assert!(session.top_rows("items", &[], 1).await.is_err());
    assert!(
        session
            .top_rows("items", &[("missing", true)], 1)
            .await
            .is_err()
    );
    assert!(
        session
            .top_rows("items", &[("id", true), ("ID", false)], 1)
            .await
            .is_err()
    );
    assert!(
        session
            .top_rows("missing", &[("id", true)], 1)
            .await
            .is_err()
    );
    Ok(())
}

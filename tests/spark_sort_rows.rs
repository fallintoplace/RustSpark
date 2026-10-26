use datafusion::arrow::array::{Array, Int64Array};
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn sort_rows_respects_multiple_keys_and_spark_null_defaults() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "items",
            "SELECT CAST(NULL AS BIGINT) AS id, CAST(5 AS BIGINT) AS score
             UNION ALL SELECT 1, 1
             UNION ALL SELECT 1, 3
             UNION ALL SELECT 2, 2",
        )
        .await?;

    let sorted = session
        .sort_rows("items", &[("id", true), ("score", false)])
        .await?;
    let batches = sorted.collect().await?;
    let mut actual = Vec::new();
    for batch in &batches {
        let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let scores = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for row in 0..batch.num_rows() {
            actual.push((
                if ids.is_null(row) { None } else { Some(ids.value(row)) },
                scores.value(row),
            ));
        }
    }
    assert_eq!(
        actual,
        [(None, 5), (Some(1), 3), (Some(1), 1), (Some(2), 2)]
    );

    let descending = session.sort_rows("items", &[("id", false)]).await?;
    let reversed = descending.collect().await?;
    let last = reversed.last().unwrap();
    let ids = last.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(ids.is_null(last.num_rows() - 1));

    assert!(session.sort_rows("items", &[]).await.is_err());
    assert!(session.sort_rows("items", &[("missing", true)]).await.is_err());
    assert!(
        session
            .sort_rows("items", &[("id", true), ("ID", false)])
            .await
            .is_err()
    );
    assert_eq!(session.table("items").await?.count().await?, 4);
    Ok(())
}

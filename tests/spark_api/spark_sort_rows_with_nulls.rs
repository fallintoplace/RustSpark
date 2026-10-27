use datafusion::{
    arrow::array::{Array, Int64Array},
    prelude::DataFrame,
};
use rustspark::RustSparkSession;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn ids(dataframe: DataFrame) -> TestResult<Vec<Option<i64>>> {
    let mut values = Vec::new();
    for batch in dataframe.collect().await? {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id must remain BIGINT");
        for row in 0..batch.num_rows() {
            values.push(if column.is_null(row) {
                None
            } else {
                Some(column.value(row))
            });
        }
    }
    Ok(values)
}

#[tokio::test]
async fn explicitly_controls_null_position_for_each_sort_direction() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "items",
            "SELECT CAST(NULL AS BIGINT) AS id
             UNION ALL SELECT 2
             UNION ALL SELECT 1",
        )
        .await?;

    let ascending = session
        .sort_rows_with_nulls("items", &[("id", true, false)])
        .await?;
    assert_eq!(ids(ascending).await?, [Some(1), Some(2), None]);

    let descending = session
        .sort_rows_with_nulls("items", &[("id", false, true)])
        .await?;
    assert_eq!(ids(descending).await?, [None, Some(2), Some(1)]);

    assert!(session.sort_rows_with_nulls("items", &[]).await.is_err());
    assert!(
        session
            .sort_rows_with_nulls("items", &[("missing", true, true)])
            .await
            .is_err()
    );
    assert!(
        session
            .sort_rows_with_nulls("items", &[("id", true, true), ("ID", false, false)])
            .await
            .is_err()
    );
    assert_eq!(session.table("items").await?.count().await?, 3);
    Ok(())
}

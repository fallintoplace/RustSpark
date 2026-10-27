use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn pages_registered_table_rows_with_checked_limits()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "numbers",
            "SELECT CAST(3 AS BIGINT) AS n UNION ALL SELECT 1 UNION ALL SELECT 2",
        )
        .await?;
    let sorted = session.sort_rows("numbers", &[("n", true)]).await?;
    session.register_dataframe("sorted", sorted)?;

    let batches = session.table_page("sorted", 1, 1).await?.collect().await?;
    assert_eq!(batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
    assert_eq!(
        ScalarValue::try_from_array(batches[0].column(0).as_ref(), 0)?,
        ScalarValue::Int64(Some(2))
    );
    assert_eq!(session.table_page("sorted", 20, 3).await?.count().await?, 0);
    assert!(session.table_page("sorted", 0, 0).await.is_err());
    assert!(session.table_page("sorted", usize::MAX, 2).await.is_err());
    assert!(session.table_page("missing", 0, 1).await.is_err());
    Ok(())
}

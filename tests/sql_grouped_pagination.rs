use datafusion::arrow::array::{Int64Array, StringArray};
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn grouped_sort_uses_offset_after_aggregation() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "entries",
            "SELECT 'alpha' AS category, 4 AS amount
             UNION ALL SELECT 'alpha', 6
             UNION ALL SELECT 'beta', 8
             UNION ALL SELECT 'gamma', 3
             UNION ALL SELECT 'delta', 11",
        )
        .await?;

    let query = "SELECT category, SUM(amount) AS total
                 FROM entries GROUP BY category
                 ORDER BY total DESC, category
                 LIMIT 2 OFFSET 1";
    let batches = session.sql(query).await?.collect().await?;
    assert_eq!(batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 2);

    let mut actual = Vec::new();
    for batch in &batches {
        let labels = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let totals = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for row in 0..batch.num_rows() {
            actual.push((labels.value(row).to_owned(), totals.value(row)));
        }
    }
    assert_eq!(actual, vec![("alpha".to_owned(), 10), ("beta".to_owned(), 8)]);

    let empty = session
        .sql(
            "SELECT category, COUNT(*) FROM entries
             GROUP BY category ORDER BY category LIMIT 10 OFFSET 1000",
        )
        .await?
        .count()
        .await?;
    assert_eq!(empty, 0);
    Ok(())
}

use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn reports_row_presence_for_tables_and_views() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_csv("sales", "tests/fixtures/sales.csv")
        .await?;
    session
        .register_sql_view("empty", "SELECT 1 AS n WHERE FALSE")
        .await?;
    session
        .register_sql_view("present", "SELECT 1 AS n UNION ALL SELECT 2 AS n")
        .await?;

    assert!(session.table_has_rows("sales").await?);
    assert!(session.table_has_rows("present").await?);
    assert!(!session.table_has_rows("empty").await?);
    assert!(session.table_has_rows("missing").await.is_err());
    Ok(())
}

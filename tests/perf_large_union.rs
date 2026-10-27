use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn union_more_than_one_lookup_window_keeps_all_inputs_in_order() -> TestResult {
    let session = RustSparkSession::new();
    let names = (0..33)
        .map(|index| format!("part_{index}"))
        .collect::<Vec<_>>();
    for (index, name) in names.iter().enumerate() {
        session
            .register_sql_view(name, &format!("SELECT {index} AS id"))
            .await?;
    }
    let borrowed = names.iter().map(String::as_str).collect::<Vec<_>>();
    let union = session.union_tables(&borrowed).await?;
    session.register_dataframe("combined", union)?;
    assert_eq!(
        session
            .sql_scalar("SELECT COUNT(*) FROM combined")
            .await?
            .to_string(),
        "33"
    );
    assert_eq!(
        session
            .sql_scalar("SELECT SUM(id) FROM combined")
            .await?
            .to_string(),
        "528"
    );
    let by_name = session.union_tables_by_name(&borrowed).await?;
    assert_eq!(by_name.count().await?, 33);
    Ok(())
}

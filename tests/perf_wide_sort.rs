use rustspark::RustSparkSession;

#[tokio::test]
async fn wide_sort_preserves_validation_and_schema() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let projection = (0..32)
        .map(|index| format!("{index} AS c{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    session
        .register_sql_view("wide", &format!("SELECT {projection}"))
        .await?;

    let names = (0..32)
        .map(|index| format!("c{index}"))
        .collect::<Vec<_>>();
    let order = names
        .iter()
        .map(|name| (name.as_str(), true, true))
        .collect::<Vec<_>>();
    let sorted = session.sort_rows_with_nulls("wide", &order).await?;
    assert_eq!(sorted.schema().fields().len(), 32);
    assert_eq!(sorted.count().await?, 1);

    let mut unknown = order;
    unknown[31].0 = "unknown";
    let error = session
        .sort_rows_with_nulls("wide", &unknown)
        .await
        .err()
        .expect("unknown sort key should fail");
    assert!(error.to_string().contains("Unknown sort column: unknown"));

    let mut duplicated = unknown;
    duplicated[31].0 = "C0";
    let error = session
        .sort_rows_with_nulls("wide", &duplicated)
        .await
        .err()
        .expect("duplicate sort key should fail");
    assert!(error.to_string().contains("Duplicate sort column: C0"));
    Ok(())
}

use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn wide_selection_preserves_names_and_validation_errors() -> TestResult {
    let session = RustSparkSession::new();
    let projection = (0..32)
        .map(|index| format!("{index} AS c{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    session
        .register_sql_view("wide", &format!("SELECT {projection}"))
        .await?;

    let names = (0..32).map(|index| format!("c{index}")).collect::<Vec<_>>();
    let selected = names.iter().map(String::as_str).collect::<Vec<_>>();
    let frame = session.select_columns("wide", &selected).await?;
    assert_eq!(frame.schema().fields().len(), 32);
    assert_eq!(frame.count().await?, 1);

    let mut unknown = selected.clone();
    unknown[31] = "missing";
    let error = session
        .select_columns("wide", &unknown)
        .await
        .err()
        .expect("unknown column should fail");
    assert!(error.to_string().contains("Unknown selected column: missing"));

    let mut duplicate = selected.clone();
    duplicate[31] = "C0";
    let error = session
        .select_columns("wide", &duplicate)
        .await
        .err()
        .expect("case-insensitive duplicate should fail");
    assert!(error.to_string().contains("Duplicate selected column: C0"));

    assert!(session.select_columns("wide", &["missing"]).await.is_err());
    Ok(())
}

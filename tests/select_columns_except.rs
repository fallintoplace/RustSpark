use rustspark::RustSparkSession;

#[tokio::test]
async fn select_columns_except_preserves_original_order() -> Result<(), Box<dyn std::error::Error>>
{
    let session = RustSparkSession::new();
    session
        .register_sql_view("items", "SELECT 1 AS id, 'north' AS region, 9 AS score")
        .await?;
    let frame = session.select_columns_except("items", &["region"]).await?;
    let fields: Vec<_> = frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(fields, ["id", "score"]);
    assert_eq!(frame.count().await?, 1);
    assert_eq!(
        session
            .select_columns_except("items", &[])
            .await?
            .schema()
            .fields()
            .len(),
        3
    );

    assert!(
        session
            .select_columns_except("items", &["unknown"])
            .await
            .is_err()
    );
    assert!(
        session
            .select_columns_except("items", &["score", "score"])
            .await
            .is_err()
    );
    assert!(
        session
            .select_columns_except("items", &["id", "region", "score"])
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn wide_select_except_handles_many_exclusions() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let projection = (0..64)
        .map(|index| format!("{index} AS c{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    session
        .register_sql_view("wide", &format!("SELECT {projection}"))
        .await?;

    let excluded = (0..48).map(|index| format!("c{index}")).collect::<Vec<_>>();
    let excluded = excluded.iter().map(String::as_str).collect::<Vec<_>>();
    let frame = session.select_columns_except("wide", &excluded).await?;
    let actual = frame.schema().fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>();
    let expected = (48..64).map(|index| format!("c{index}")).collect::<Vec<_>>();
    assert_eq!(actual, expected.iter().map(String::as_str).collect::<Vec<_>>());
    assert_eq!(frame.count().await?, 1);
    Ok(())
}

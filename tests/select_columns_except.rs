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

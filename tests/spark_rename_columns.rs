use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn rename_columns_supports_atomic_swaps() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "numbers",
            "SELECT CAST(1 AS BIGINT) AS left_col, CAST(2 AS BIGINT) AS right_col",
        )
        .await?;

    let swapped = session
        .rename_columns(
            "numbers",
            &[("left_col", "right_col"), ("right_col", "left_col")],
        )
        .await?;
    let names: Vec<_> = swapped
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(names, ["right_col", "left_col"]);
    assert_eq!(swapped.count().await?, 1);
    assert_eq!(session.describe_table("numbers").await?[0].0, "left_col");

    assert!(
        session
            .rename_columns("numbers", &[("left_col", "right_col")])
            .await
            .is_err()
    );
    assert!(
        session
            .rename_columns("numbers", &[("missing", "renamed")])
            .await
            .is_err()
    );
    assert!(
        session
            .rename_columns("numbers", &[("left_col", "x"), ("left_col", "y")])
            .await
            .is_err()
    );
    assert_eq!(
        session
            .rename_columns("numbers", &[])
            .await?
            .count()
            .await?,
        1
    );
    Ok(())
}

use rustspark::{NullDropMode, RustSparkSession};

#[tokio::test]
async fn wide_schema_validates_many_requested_columns() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let projections = (0..64)
        .map(|index| format!("CAST({index} AS BIGINT) AS col_{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    session
        .register_sql_view("wide", &format!("SELECT {projections}"))
        .await?;

    let names = (0..32)
        .map(|index| format!("col_{index}"))
        .collect::<Vec<_>>();
    let refs = names.iter().map(String::as_str).collect::<Vec<_>>();
    let filtered = session
        .drop_null_rows("wide", &refs, NullDropMode::Any)
        .await?;
    assert_eq!(filtered.schema().fields().len(), 64);
    let distinct = session.drop_duplicates("wide", &refs).await?;
    assert_eq!(distinct.schema().fields().len(), 64);

    let renamed = session
        .rename_columns("wide", &[("col_0", "renamed_0"), ("col_63", "last")])
        .await?;
    assert_eq!(renamed.schema().fields()[0].name(), "renamed_0");
    assert_eq!(renamed.schema().fields()[63].name(), "last");

    assert!(
        session
            .drop_null_rows("wide", &["Col_1"], NullDropMode::Any)
            .await
            .is_err()
    );
    Ok(())
}

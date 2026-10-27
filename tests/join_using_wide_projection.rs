use datafusion::logical_expr::JoinType;
use rustspark::RustSparkSession;

#[tokio::test]
async fn wide_using_join_preserves_projection_order() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let left = (0..32)
        .map(|n| format!("{n} AS l{n}"))
        .collect::<Vec<_>>()
        .join(", ");
    let right = (0..32)
        .map(|n| format!("{n} AS r{n}"))
        .collect::<Vec<_>>()
        .join(", ");
    session
        .register_sql_view("wide_left", &format!("SELECT 1 AS id, {left}"))
        .await?;
    session
        .register_sql_view("wide_right", &format!("SELECT 1 AS id, {right}"))
        .await?;
    let frame = session
        .join_tables_using("wide_left", "wide_right", JoinType::Inner, &["id"])
        .await?;
    assert_eq!(frame.schema().fields().len(), 65);
    assert_eq!(frame.schema().field(0).name(), "id");
    assert_eq!(frame.schema().field(1).name(), "l0");
    assert_eq!(frame.schema().field(33).name(), "r0");
    assert_eq!(frame.count().await?, 1);
    Ok(())
}

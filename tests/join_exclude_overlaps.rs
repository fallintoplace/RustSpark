use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn interval_anti_join_preserves_unmatched_groups_and_empty_intervals() -> TestResult {
    let session = RustSparkSession::new();
    session.register_sql_view(
        "blocks",
        "SELECT 'east' AS region, CAST(0 AS BIGINT) AS start_at, CAST(5 AS BIGINT) AS end_at
         UNION ALL SELECT 'east', 5, 8
         UNION ALL SELECT 'west', 0, 5
         UNION ALL SELECT 'east', CAST(NULL AS BIGINT), 5
         UNION ALL SELECT 'east', 10, 10",
    ).await?;
    session.register_sql_view(
        "reserved",
        "SELECT 'east' AS area, CAST(4 AS BIGINT) AS begin_at, CAST(6 AS BIGINT) AS finish_at
         UNION ALL SELECT 'east', 8, 12",
    ).await?;
    let bounds_l = ("start_at", "end_at");
    let bounds_r = ("begin_at", "finish_at");
    assert_eq!(
        session.exclude_overlapping_intervals(
            "blocks","reserved",bounds_l,bounds_r,&[("region","area")]
        ).await?.count().await?,
        3
    );
    assert_eq!(
        session.exclude_overlapping_intervals(
            "blocks","reserved",bounds_l,bounds_r,&[]
        ).await?.count().await?,
        2
    );
    Ok(())
}

#[tokio::test]
async fn interval_anti_join_validates_columns_and_group_keys() -> TestResult {
    let session = RustSparkSession::new();
    session.register_sql_view("l", "SELECT 0 AS lo, 10 AS hi, 1 AS id").await?;
    session.register_sql_view("r", "SELECT 5 AS lo, 15 AS hi, 1 AS id").await?;
    assert!(
        session.exclude_overlapping_intervals(
            "l","r",("lo","hi"),("lo","hi"),&[("id","")]
        ).await.is_err()
    );
    assert!(
        session.exclude_overlapping_intervals(
            "l","r",("missing","hi"),("lo","hi"),&[]
        ).await.is_err()
    );
    Ok(())
}

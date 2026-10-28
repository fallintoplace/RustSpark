use rustspark::{ExistenceFilter, ExistenceMode, RustSparkSession};

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn fixture() -> Result<RustSparkSession, Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT CAST(1 AS BIGINT) AS id, 'us' AS region
         UNION ALL SELECT 2, 'eu'
         UNION ALL SELECT 3, 'us'
         UNION ALL SELECT CAST(NULL AS BIGINT), 'us'
         UNION ALL SELECT 1, 'eu'",
        )
        .await?;
    session
        .register_sql_view(
            "allowed",
            "SELECT CAST(1 AS BIGINT) AS ref_id, 'us' AS region
         UNION ALL SELECT 1, 'us'
         UNION ALL SELECT 2, 'eu'",
        )
        .await?;
    session
        .register_sql_view(
            "blocked",
            "SELECT CAST(2 AS BIGINT) AS ref_id, 'eu' AS region",
        )
        .await?;
    Ok(session)
}

#[tokio::test]
async fn matches_three_tables_without_multiplying_duplicate_lookup_rows() -> TestResult {
    let session = fixture().await?;
    let rows = session
        .filter_table_by_existence(
            "events",
            &[
                ExistenceFilter {
                    table: "allowed",
                    mode: ExistenceMode::Exists,
                    keys: &[("id", "ref_id"), ("region", "region")],
                },
                ExistenceFilter {
                    table: "blocked",
                    mode: ExistenceMode::NotExists,
                    keys: &[("id", "ref_id"), ("region", "region")],
                },
            ],
        )
        .await?
        .count()
        .await?;
    assert_eq!(rows, 1);
    Ok(())
}

#[tokio::test]
async fn anti_existence_preserves_null_key_rows() -> TestResult {
    let session = fixture().await?;
    let rows = session
        .filter_table_by_existence(
            "events",
            &[ExistenceFilter {
                table: "blocked",
                mode: ExistenceMode::NotExists,
                keys: &[("id", "ref_id")],
            }],
        )
        .await?
        .count()
        .await?;
    assert_eq!(rows, 4);
    Ok(())
}

#[tokio::test]
async fn rejects_missing_filters_and_invalid_predicates() -> TestResult {
    let session = fixture().await?;
    assert!(
        session
            .filter_table_by_existence("events", &[])
            .await
            .is_err()
    );
    for filter in [
        ExistenceFilter {
            table: "allowed",
            mode: ExistenceMode::Exists,
            keys: &[],
        },
        ExistenceFilter {
            table: "allowed",
            mode: ExistenceMode::Exists,
            keys: &[("", "ref_id")],
        },
        ExistenceFilter {
            table: "missing",
            mode: ExistenceMode::Exists,
            keys: &[("id", "ref_id")],
        },
    ] {
        assert!(
            session
                .filter_table_by_existence("events", &[filter])
                .await
                .is_err()
        );
    }
    Ok(())
}

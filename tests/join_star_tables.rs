use datafusion::logical_expr::JoinType;
use rustspark::{RustSparkSession, StarDimension};

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn fixture() -> Result<RustSparkSession, Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "fact",
            "SELECT 1 AS customer_id, 10 AS product_id
                 UNION ALL SELECT 2, 20
                 UNION ALL SELECT 3, 30",
        )
        .await?;
    session
        .register_sql_view(
            "customers",
            "SELECT 1 AS id, 'eu' AS region
                      UNION ALL SELECT 3, 'us'",
        )
        .await?;
    session
        .register_sql_view(
            "products",
            "SELECT 10 AS id, 'food' AS category
                     UNION ALL SELECT 20, 'tools'",
        )
        .await?;
    Ok(session)
}

#[tokio::test]
async fn joins_a_fact_to_two_dimensions_with_safe_output_names() -> TestResult {
    let session = fixture().await?;
    let frame = session
        .join_star_tables(
            "fact",
            &[
                StarDimension {
                    table: "customers",
                    alias: "customer",
                    join_type: JoinType::Left,
                    keys: &[("customer_id", "id")],
                    columns: &["region"],
                },
                StarDimension {
                    table: "products",
                    alias: "product",
                    join_type: JoinType::Left,
                    keys: &[("product_id", "id")],
                    columns: &["category"],
                },
            ],
        )
        .await?;
    let columns: Vec<_> = frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(
        columns,
        [
            "customer_id",
            "product_id",
            "customer__region",
            "product__category"
        ]
    );
    assert_eq!(frame.count().await?, 3);
    Ok(())
}

#[tokio::test]
async fn supports_inner_dimension_filters() -> TestResult {
    let session = fixture().await?;
    let frame = session
        .join_star_tables(
            "fact",
            &[
                StarDimension {
                    table: "customers",
                    alias: "c",
                    join_type: JoinType::Left,
                    keys: &[("customer_id", "id")],
                    columns: &["region"],
                },
                StarDimension {
                    table: "products",
                    alias: "p",
                    join_type: JoinType::Inner,
                    keys: &[("product_id", "id")],
                    columns: &["category"],
                },
            ],
        )
        .await?;
    assert_eq!(frame.count().await?, 2);
    Ok(())
}

#[tokio::test]
async fn rejects_ambiguous_aliases_and_unknown_keys_or_columns() -> TestResult {
    let session = fixture().await?;
    assert!(session.join_star_tables("fact", &[]).await.is_err());
    for dimension in [
        StarDimension {
            table: "customers",
            alias: "__rustspark_fact",
            join_type: JoinType::Left,
            keys: &[("customer_id", "id")],
            columns: &["region"],
        },
        StarDimension {
            table: "customers",
            alias: "c",
            join_type: JoinType::Full,
            keys: &[("customer_id", "id")],
            columns: &["region"],
        },
        StarDimension {
            table: "customers",
            alias: "c",
            join_type: JoinType::Left,
            keys: &[("missing", "id")],
            columns: &["region"],
        },
        StarDimension {
            table: "customers",
            alias: "c",
            join_type: JoinType::Left,
            keys: &[("customer_id", "id")],
            columns: &["missing"],
        },
    ] {
        assert!(
            session
                .join_star_tables("fact", &[dimension])
                .await
                .is_err()
        );
    }
    Ok(())
}

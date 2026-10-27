use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn built_in_datetime_string_and_regex_functions_are_queryable() -> TestResult {
    let session = RustSparkSession::new();
    let rows = session
        .sql(
            "SELECT value FROM (VALUES (1)) AS source(value)
             WHERE regexp_replace('ab123', '[0-9]+', 'X') = 'abX'
               AND octet_length('café') = 5
               AND extract(minute FROM to_timestamp_seconds(125)) = 2
               AND date_trunc('minute', to_timestamp_seconds(125)) =
                   to_timestamp_seconds(120)",
        )
        .await?
        .count()
        .await?;

    assert_eq!(rows, 1);
    Ok(())
}

#[tokio::test]
async fn unicode_functions_remain_available_with_other_scalar_functions() -> TestResult {
    let session = RustSparkSession::new();
    let rows = session
        .sql(
            "SELECT value FROM (VALUES (1)) AS source(value)
             WHERE character_length('café') = 4
               AND substr('abcdef', 2, 3) = 'bcd'",
        )
        .await?
        .count()
        .await?;

    assert_eq!(rows, 1);
    Ok(())
}

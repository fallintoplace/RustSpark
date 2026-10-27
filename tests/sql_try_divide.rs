use datafusion::{
    arrow::array::Float64Array,
    prelude::{SessionConfig, SessionContext},
};
use rustspark::RustSparkSession;

#[tokio::test]
async fn try_divide_handles_integer_inputs_and_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT TRY_DIVIDE(n, d) AS result FROM \
             (VALUES (0, 9, 2), (1, 4, 0), (2, NULL, 3), (3, -6, 3)) AS t(idx, n, d) \
             ORDER BY idx",
        )
        .await?
        .collect()
        .await?;
    let mut result = Vec::new();
    for batch in batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        result.extend(values.iter());
    }
    assert_eq!(result, vec![Some(4.5), None, None, Some(-2.0)]);

    let singleton = session
        .sql("SELECT TRY_DIVIDE(7, 2)")
        .await?
        .collect()
        .await?;
    let values = singleton[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(values.value(0), 3.5);
    Ok(())
}

#[tokio::test]
async fn try_divide_is_registered_in_all_public_session_constructors()
-> Result<(), Box<dyn std::error::Error>> {
    let configured = RustSparkSession::with_config(SessionConfig::new());
    assert!(configured.sql("SELECT try_divide(1, 0)").await.is_ok());
    let wrapped = RustSparkSession::from_context(SessionContext::new());
    assert!(wrapped.sql("SELECT try_divide(3, 2)").await.is_ok());
    Ok(())
}

#[test]
fn try_divide_works_in_cli() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT TRY_DIVIDE(9, 2)",
            "--stdout-format",
            "json-array",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("4.5"));
    Ok(())
}

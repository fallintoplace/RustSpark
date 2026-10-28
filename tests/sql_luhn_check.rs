use datafusion::arrow::array::BooleanArray;
use rustspark::RustSparkSession;

#[tokio::test]
async fn luhn_check_handles_rows_and_nulls() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let batches = session
        .sql(
            "SELECT LUHN_CHECK(value) FROM \
             (VALUES (0, '79927398713'), (1, '79927398714'), (2, '123x'), (3, NULL), (4, '')) AS t(idx, value) ORDER BY idx",
        )
        .await?
        .collect()
        .await?;
    let mut actual = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        actual.extend(column.iter());
    }
    assert_eq!(
        actual,
        vec![Some(true), Some(false), Some(false), None, Some(false)]
    );
    let scalar = session
        .sql("SELECT LUHN_CHECK('0')")
        .await?
        .collect()
        .await?;
    let values = scalar[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(values.value(0), true);
    Ok(())
}

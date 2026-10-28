use std::sync::Arc;

use datafusion::{
    arrow::{array::{Int64Array, Date32Array}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

use crate::sql_compat_spark_dates;

/// Construct a Date32 from Unix epoch days.
fn calculate(days: i64) -> Option<i32> {
    let days = i32::try_from(days).ok()?;
    sql_compat_spark_dates::date_from_days(days).map(|_| days)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "date_from_unix_date",
        vec![DataType::Int64],
        DataType::Date32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Int64(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Date32(
                    (*value).and_then(calculate),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0].as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("date_from_unix_date expects Int64".into()))?;
            let result: Date32Array = input.iter().map(|value| value.and_then(calculate)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use crate::RustSparkSession;
    use datafusion::arrow::array::StringArray;

    #[tokio::test]
    async fn respects_spark_calendar_values_and_nulls() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT CAST(DATE_FROM_UNIX_DATE(days) AS VARCHAR) FROM (VALUES (0, CAST(0 AS BIGINT)), (1, CAST(-1 AS BIGINT)), (2, CAST(1 AS BIGINT)), (3, CAST(NULL AS BIGINT))) AS t(id,days) ORDER BY id")
            .await?.collect().await?;
        let actual: Vec<_> = batches.iter().flat_map(|batch| {
            let values = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            values.iter().map(|value| value.map(str::to_owned)).collect::<Vec<_>>()
        }).collect();
        assert_eq!(actual, vec![Some("1970-01-01".to_owned()), Some("1969-12-31".to_owned()), Some("1970-01-02".to_owned()), None]);
        Ok(())
    }
}

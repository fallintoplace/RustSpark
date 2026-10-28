use std::sync::Arc;

use datafusion::{
    arrow::{array::{Date32Array, Int32Array}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

use crate::sql_compat_spark_dates;

/// Convert a date to whole days since 1970-01-01.
fn calculate(days: i32) -> Option<i32> {
    sql_compat_spark_dates::date_from_days(days).map(|_| days)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "unix_date",
        vec![DataType::Date32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Date32(arg0))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int32(
                    (*arg0).and_then(calculate),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let arg0 = arrays[0].as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("unix_date expects Date32 argument 1".into()))?;
            let result: Int32Array = arg0.iter().map(|v| v.and_then(calculate)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use crate::RustSparkSession;
    use datafusion::arrow::array::Int32Array;

    #[tokio::test]
    async fn handles_calendar_boundaries_and_nulls() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT UNIX_DATE(d) FROM (VALUES (0, DATE '1970-01-01'), (1, DATE '1969-12-31'), (2, DATE '1970-01-02'), (3, CAST(NULL AS DATE))) AS t(id,d) ORDER BY id")
            .await?.collect().await?;
        let actual: Vec<_> = batches.iter().flat_map(|batch| {
            let values = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            values.iter().collect::<Vec<_>>()
        }).collect();
        assert_eq!(actual, vec![Some(0), Some(-1), Some(1), None]);
        Ok(())
    }
}

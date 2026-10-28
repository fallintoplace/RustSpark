use std::sync::Arc;

use datafusion::{
    arrow::{array::{Date32Array, Int64Array}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

use crate::sql_compat_spark_dates;

/// Subtract whole days with checked Arrow Date32 arithmetic.
fn calculate(days: i32, amount: i64) -> Option<i32> {
    let delta = i32::try_from(amount).ok()?;
    let result = days.checked_sub(delta)?;
    sql_compat_spark_dates::date_from_days(result).map(|_| result)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "date_sub",
        vec![DataType::Date32, DataType::Int64],
        DataType::Date32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Date32(arg0)),
                ColumnarValue::Scalar(ScalarValue::Int64(arg1))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Date32(
                    (*arg0).zip(*arg1).and_then(|(a,b)| calculate(a,b)),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let arg0 = arrays[0].as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("date_sub expects Date32 argument 1".into()))?;
            let arg1 = arrays[1].as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("date_sub expects Int64 argument 2".into()))?;
            let result: Date32Array = arg0.iter().zip(arg1.iter()).map(|(a,b)| a.zip(b).and_then(|(a,b)| calculate(a,b))).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use crate::RustSparkSession;
    use datafusion::arrow::array::StringArray;

    #[tokio::test]
    async fn handles_calendar_boundaries_and_nulls() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT CAST(DATE_SUB(d, n) AS VARCHAR) FROM (VALUES (0, DATE '2024-02-29', 1), (1, DATE '2024-03-01', 1), (2, CAST(NULL AS DATE), 2), (3, DATE '1970-01-01', 1)) AS t(id,d,n) ORDER BY id")
            .await?.collect().await?;
        let actual: Vec<_> = batches.iter().flat_map(|batch| {
            let values = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            values.iter().map(|v| v.map(str::to_owned)).collect::<Vec<_>>()
        }).collect();
        assert_eq!(actual, vec![Some("2024-02-28".to_owned()), Some("2024-02-29".to_owned()), None, Some("1969-12-31".to_owned())]);
        Ok(())
    }
}

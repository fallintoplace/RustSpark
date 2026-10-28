use std::sync::Arc;

use datafusion::{
    arrow::{array::{Date32Array, Int32Array}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

use crate::sql_compat_spark_dates;

/// Difference in days, end date minus start date.
fn calculate(end: i32, start: i32) -> Option<i32> {
    sql_compat_spark_dates::date_from_days(end)?;
    sql_compat_spark_dates::date_from_days(start)?;
    end.checked_sub(start)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "datediff",
        vec![DataType::Date32, DataType::Date32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Date32(arg0)),
                ColumnarValue::Scalar(ScalarValue::Date32(arg1))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int32(
                    (*arg0).zip(*arg1).and_then(|(a,b)| calculate(a,b)),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let arg0 = arrays[0].as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("datediff expects Date32 argument 1".into()))?;
            let arg1 = arrays[1].as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("datediff expects Date32 argument 2".into()))?;
            let result: Int32Array = arg0.iter().zip(arg1.iter()).map(|(a,b)| a.zip(b).and_then(|(a,b)| calculate(a,b))).collect();
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
            .sql("SELECT DATEDIFF(e,s) FROM (VALUES (0, DATE '2024-03-01', DATE '2024-02-29'), (1, DATE '2024-02-28', DATE '2024-03-01'), (2, CAST(NULL AS DATE), DATE '2024-01-01'), (3, DATE '1970-01-01', DATE '1969-12-31')) AS t(id,e,s) ORDER BY id")
            .await?.collect().await?;
        let actual: Vec<_> = batches.iter().flat_map(|batch| {
            let values = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            values.iter().collect::<Vec<_>>()
        }).collect();
        assert_eq!(actual, vec![Some(1), Some(-2), None, Some(1)]);
        Ok(())
    }
}

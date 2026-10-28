use std::sync::Arc;

use datafusion::{
    arrow::{array::{Date32Array, Int32Array}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

use crate::sql_compat_spark_dates;

use chrono::Datelike;

/// Extract the signed proleptic Gregorian year from a Date32.
fn calculate(days: i32) -> Option<i32> {
    Some(sql_compat_spark_dates::date_from_days(days)?.year())
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "year",
        vec![DataType::Date32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Date32(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int32((*value).and_then(calculate))));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0].as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("year expects Date32".into()))?;
            let result: Int32Array = input.iter().map(|v| v.and_then(calculate)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use crate::RustSparkSession;
    use datafusion::arrow::array::Int32Array;

    #[tokio::test]
    async fn handles_spark_boundary_dates_and_nulls() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new().sql("SELECT YEAR(d) FROM (VALUES (0, DATE '1969-12-31'), (1, DATE '2024-02-29'), (2, CAST(NULL AS DATE))) AS t(id,d) ORDER BY id")
            .await?.collect().await?;
        let actual: Vec<_> = batches.iter().flat_map(|batch| {
            let v = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            v.iter().collect::<Vec<_>>()
        }).collect();
        assert_eq!(actual, vec![Some(1969), Some(2024), None]);
        Ok(())
    }
}

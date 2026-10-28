use std::sync::Arc;

use chrono::Datelike;
use datafusion::{
    arrow::{array::{Date32Array, Float64Array}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

use crate::sql_compat_spark_dates;

/// Spark's date-only MONTHS_BETWEEN uses whole months on matching days
/// or for two month-end dates. Otherwise it treats a month as 31 days,
/// and rounds the fractional result to eight decimal places.
fn calculate(first: i32, second: i32) -> Option<f64> {
    let first = sql_compat_spark_dates::date_from_days(first)?;
    let second = sql_compat_spark_dates::date_from_days(second)?;
    let full_months = i64::from(first.year() - second.year()) * 12
        + i64::from(first.month()) - i64::from(second.month());
    let both_month_end = sql_compat_spark_dates::last_day_of_month(first)? == first
        && sql_compat_spark_dates::last_day_of_month(second)? == second;
    if first.day() == second.day() || both_month_end {
        return Some(full_months as f64);
    }
    let fraction = (f64::from(first.day()) - f64::from(second.day())) / 31.0;
    let result = full_months as f64 + fraction;
    Some((result * 100_000_000.0).round() / 100_000_000.0)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "months_between",
        vec![DataType::Date32, DataType::Date32],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [
                ColumnarValue::Scalar(ScalarValue::Date32(first)),
                ColumnarValue::Scalar(ScalarValue::Date32(second)),
            ] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                    (*first).zip(*second).and_then(|(a,b)| calculate(a,b)),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let first = arrays[0].as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("months_between expects Date32".into()))?;
            let second = arrays[1].as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("months_between expects Date32".into()))?;
            let result: Float64Array = first.iter().zip(second.iter())
                .map(|(a,b)| a.zip(b).and_then(|(a,b)| calculate(a,b)))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use crate::RustSparkSession;
    use datafusion::arrow::array::Float64Array;

    #[tokio::test]
    async fn supports_month_end_fractional_negative_and_null_dates() -> datafusion::error::Result<()> {
        let query = "SELECT MONTHS_BETWEEN(a,b) FROM (VALUES (0, DATE '2024-03-31', DATE '2024-02-29'), (1, DATE '2024-03-15', DATE '2024-02-01'), (2, DATE '2024-02-01', DATE '2024-03-15'), (3, CAST(NULL AS DATE), DATE '2024-02-01')) AS t(id,a,b) ORDER BY id";
        let batches = RustSparkSession::new().sql(query).await?.collect().await?;
        let actual: Vec<_> = batches.iter().flat_map(|batch| {
            let values = batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
            values.iter().collect::<Vec<_>>()
        }).collect();
        assert_eq!(actual.len(), 4);
        assert_eq!(actual[0], Some(1.0));
        assert!((actual[1].unwrap() - 1.4516129).abs() < 0.00000001);
        assert!((actual[2].unwrap() + 1.4516129).abs() < 0.00000001);
        assert_eq!(actual[3], None);
        Ok(())
    }
}

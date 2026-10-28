use std::sync::Arc;

use chrono::Months;
use datafusion::{
    arrow::{array::{Date32Array, Int64Array}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

use crate::sql_compat_spark_dates;

fn calculate(days: i32, months: i64) -> Option<i32> {
    let date = sql_compat_spark_dates::date_from_days(days)?;
    let shifted = if months < 0 {
        date.checked_sub_months(Months::new(u32::try_from(months.checked_abs()?).ok()?))?
    } else {
        date.checked_add_months(Months::new(u32::try_from(months).ok()?))?
    };
    sql_compat_spark_dates::days_from_date(shifted)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "add_months",
        vec![DataType::Date32, DataType::Int64],
        DataType::Date32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [
                ColumnarValue::Scalar(ScalarValue::Date32(date)),
                ColumnarValue::Scalar(ScalarValue::Int64(months)),
            ] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Date32(
                    (*date).zip(*months).and_then(|(d,m)| calculate(d,m)),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let dates = arrays[0].as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("add_months expects Date32".into()))?;
            let months = arrays[1].as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("add_months expects Int64".into()))?;
            let result: Date32Array = dates.iter().zip(months.iter())
                .map(|(d,m)| d.zip(m).and_then(|(d,m)| calculate(d,m)))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use crate::RustSparkSession;
    use datafusion::arrow::array::StringArray;

    #[tokio::test]
    async fn clamps_end_of_month_and_handles_leap_years() -> datafusion::error::Result<()> {
        let query = "SELECT CAST(ADD_MONTHS(d,m) AS VARCHAR) FROM (VALUES (0, DATE '2024-01-31', 1), (1, DATE '2023-01-31', 1), (2, DATE '2024-12-31', 2), (3, CAST(NULL AS DATE), 1)) AS t(id,d,m) ORDER BY id";
        let batches = RustSparkSession::new().sql(query).await?.collect().await?;
        let actual: Vec<_> = batches.iter().flat_map(|batch| {
            let v = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            v.iter().map(|v| v.map(str::to_owned)).collect::<Vec<_>>()
        }).collect();
        assert_eq!(actual, vec![Some("2024-02-29".to_owned()), Some("2023-02-28".to_owned()), Some("2025-02-28".to_owned()), None]);
        Ok(())
    }
}
